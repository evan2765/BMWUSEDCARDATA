using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Text.Json;
using System.Globalization;
using Microsoft.Playwright;

class Program
{
    // Toggle this to watch what happens
    const bool Headless = true;

    // Faster runs: don't actually download images on /vehicle/* pages.
    // We still extract the image URL from og:image, <picture> srcset, <img> attrs, or JSON-LD.
    const bool AllowVehicleImages = false;

    static readonly string HomeUrl     = "https://usedcars.bmw.co.uk/";
    static readonly string ResultsBase = "https://usedcars.bmw.co.uk/result/";

    // Single files (always overwrite URLs; append to Specs)
    static readonly string OutDir            = Path.GetFullPath(AppContext.BaseDirectory);
    static readonly string SpecsCsvPath      = Path.Combine(OutDir, "BMWusedcarsSPECS.csv");
    static readonly string UrlsCsvPath       = Path.Combine(OutDir, "BMWusedcarsURLS.csv");
    static readonly string RemovedCsvPath    = Path.Combine(OutDir, "BMWusedcarsREMOVED.csv"); // removed/soft-404 log

    static readonly Random _rng = new();

    // Navigation pacing (results pages)
    static readonly object _navLock = new();
    static DateTime _lastNav = DateTime.MinValue;
    const int MinNavGapMs = 3500;

    // VEHICLE PACER: shared pacing + cooldown for /vehicle/*
    static readonly object _vehNavLock = new();
    static DateTime _lastVehicleNav = DateTime.MinValue;
    static DateTime _vehicleBlockedUntilUtc = DateTime.MinValue;
    const int VehicleMinNavGapMs = 900;            // tweak if you still see 429s (1100–1300)
    const int VehicleJitterMinMs = 80, VehicleJitterMaxMs = 160;

    const bool Verbose429Logs = false;

    const int PageSettleMinMs = 3000;
    const int PageSettleMaxMs = 4500;

    static volatile bool _earlyStopRequested = false;

    // ----- RUN MODE -----
    enum RunMode { Auto, ScrapeFromCsvOnly, CollectThenScrape, CollectOnly }
    // Fast testing: start from existing URL CSV
    const RunMode DefaultMode = RunMode.CollectThenScrape;

    public static async Task Main(string[] args)
    {
        var mode = ParseMode(args) ?? DefaultMode;
        bool forceRescrape = HasArg(args, "--force-rescrape");

        using var pw = await Playwright.CreateAsync();

        // ✅ Launch: no IgnoreHttpsErrors here
        await using var browser = await pw.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
        {
            Headless = Headless
        });

// ✅ Context: put it here
        var ctx = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            ViewportSize = null,
            IgnoreHTTPSErrors = true,   // ✅ correct property name
            UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        });
        // Single route that (a) blocks heavy assets and (b) only allows images on /vehicle/* if enabled
        await ctx.RouteAsync("**/*", async route =>
        {
            var req = route.Request;
            var rt = req.ResourceType;
            var frameUrl = req.Frame?.Url ?? "";

            // Block heavy assets globally
            if (rt == "font" || rt == "media")
            {
                await route.AbortAsync(RequestAbortErrorCode.Failed);
                return;
            }

            if (rt == "image")
            {
                if (!AllowVehicleImages)
                {
                    await route.AbortAsync(RequestAbortErrorCode.Failed);
                    return;
                }

                // Allow images only within /vehicle/* pages when AllowVehicleImages == true
                if (!frameUrl.Contains("/vehicle/", StringComparison.OrdinalIgnoreCase))
                {
                    await route.AbortAsync(RequestAbortErrorCode.Failed);
                    return;
                }
            }

            await route.ContinueAsync();
        });

        ctx.SetDefaultTimeout(20000);
        ctx.SetDefaultNavigationTimeout(30000);

        // Ensure no sticky filters from a previous run
        try { await ctx.ClearCookiesAsync(); } catch { }

        List<string> allUrls;

        if (mode == RunMode.ScrapeFromCsvOnly)
        {
            allUrls = LoadUrlsCsv(UrlsCsvPath);
            if (allUrls.Count == 0)
            {
                Console.WriteLine($"❌ ScrapeFromCsvOnly: No URLs found in {UrlsCsvPath}. Run in CollectThenScrape first.");
                await ctx.CloseAsync();
                await browser.CloseAsync();
                return;
            }
            Console.WriteLine($"📄 ScrapeFromCsvOnly: using {allUrls.Count:N0} URLs from {UrlsCsvPath}.");
        }
        else if (mode == RunMode.CollectOnly)
        {
            allUrls = await CollectUrlsFlow(ctx);
            SaveUrlsCsvFresh(UrlsCsvPath, allUrls);
            Console.WriteLine($"💾 Saved fresh URL list to {UrlsCsvPath}. Exiting (CollectOnly).");
            await ctx.CloseAsync();
            await browser.CloseAsync();
            return;
        }
        else if (mode == RunMode.CollectThenScrape)
        {
            allUrls = await CollectUrlsFlow(ctx);
            SaveUrlsCsvFresh(UrlsCsvPath, allUrls); // overwrite each time
            Console.WriteLine($"💾 Saved fresh URL list to {UrlsCsvPath}.");
        }
        else // Auto
        {
            if (File.Exists(UrlsCsvPath))
            {
                allUrls = LoadUrlsCsv(UrlsCsvPath);
                if (allUrls.Count > 0)
                {
                    Console.WriteLine($"📄 Auto: using cached URL list from {UrlsCsvPath} ({allUrls.Count:N0} URLs).");
                }
                else
                {
                    Console.WriteLine($"⚠️ Auto: {UrlsCsvPath} had no URLs, collecting fresh.");
                    allUrls = await CollectUrlsFlow(ctx);
                    SaveUrlsCsvFresh(UrlsCsvPath, allUrls);
                    Console.WriteLine($"💾 Saved fresh URL list to {UrlsCsvPath}.");
                }
            }
            else
            {
                allUrls = await CollectUrlsFlow(ctx);
                SaveUrlsCsvFresh(UrlsCsvPath, allUrls);
                Console.WriteLine($"💾 Saved fresh URL list to {UrlsCsvPath}.");
            }
        }

        // -------- SCRAPE VEHICLE PAGES (SEQUENTIAL) — ONLY NEW --------
        EnsureSpecsCsvHeader(SpecsCsvPath);
        EnsureRemovedCsvHeader(RemovedCsvPath); // ensure header for removed log

        // Prune old rows not present in the latest URL list (pre-scrape)
        var liveIds = BuildLiveIdsFromUrls(allUrls);
        int removedPre = PruneSpecsCsvByLiveIds(SpecsCsvPath, liveIds);
        if (removedPre > 0)
            Console.WriteLine($"🧹 Pre-scrape prune: removed {removedPre:N0} rows not in the latest URL list.");

        var alreadyScraped = LoadScrapedIdsFromSpecs(SpecsCsvPath);
        Console.WriteLine($"ℹ️ Found {alreadyScraped.Count:N0} vehicles already in specs CSV.");

        // Build definitive list for THIS run
        var toScrape = new List<string>();
        foreach (var raw in allUrls)
        {
            var id = ExtractVehicleId(NormalizeVehicleUrl(raw));
            if (forceRescrape || string.IsNullOrEmpty(id) || !alreadyScraped.Contains(id))
                toScrape.Add(raw);
        }
        int skipped = allUrls.Count - toScrape.Count;
        Console.WriteLine($"🟢 Will scrape {toScrape.Count:N0} vehicles this run (skipping {skipped:N0} already in specs).");

        int doneNew = 0;
        IPage? page = null;

        try
        {
            page = await ctx.NewPageAsync();

            foreach (var raw in toScrape)
            {
                var navUrl = NormalizeVehicleUrl(raw);
                var id = ExtractVehicleId(navUrl);

                try
                {
                    await WaitVehicleNavSlotAsync();

                    var resp = await GoWithRetry(page, navUrl, maxAttempts: 5, throttle: false, isVehicle: true, quiet429: !Verbose429Logs);
                    if (resp is null)
                    {
                        Console.WriteLine($"✖ Failed to load: {navUrl}");
                        continue;
                    }

                    // quick exit on real HTTP errors
                    var statusCode = resp.Status;
                    if (statusCode == 404 || statusCode == 410)
                    {
                        AppendRemovedRow(RemovedCsvPath, id, navUrl, "HTTP gone", statusCode);
                        Console.WriteLine($"⚠️ Removed: {id} — HTTP {statusCode} (gone).");
                        continue;
                    }
                    if (statusCode >= 500 && statusCode < 600)
                    {
                        AppendRemovedRow(RemovedCsvPath, id, navUrl, "HTTP 5xx", statusCode);
                        Console.WriteLine($"⚠️ Removed: {id} — HTTP {statusCode} (server error).");
                        continue;
                    }

                    await AcceptCookies(page);

                    // soft-404 template detection
                    if (await IsSoft404Async(page))
                    {
                        AppendRemovedRow(RemovedCsvPath, id, navUrl, "Soft 404 (template)", statusCode);
                        Console.WriteLine($"⚠️ Removed: {id} — Soft 404 (template).");
                        continue;
                    }

                    // DOM readiness checks
                    if (!await EnsureVehicleDom(page, 8000))
                    {
                        try
                        {
                            await page.ReloadAsync(new() { WaitUntil = WaitUntilState.DOMContentLoaded, Timeout = 20000 });
                            await AcceptCookies(page);
                        }
                        catch { }

                        // check soft-404 again after reload
                        if (await IsSoft404Async(page))
                        {
                            AppendRemovedRow(RemovedCsvPath, id, navUrl, "Soft 404 (template)", statusCode);
                            Console.WriteLine($"⚠️ Removed: {id} — Soft 404 (template) after reload.");
                            continue;
                        }

                        if (!await EnsureVehicleDom(page, 6000))
                        {
                            AppendRemovedRow(RemovedCsvPath, id, navUrl, "No vehicle DOM", statusCode);
                            Console.WriteLine($"⚠️ Removed: {id} — No vehicle DOM.");
                            continue;
                        }
                    }

                    // small stabilisation dwell (~0.5s)
                    await page.WaitForTimeoutAsync(_rng.Next(450, 650));

                    // make sure spec values (lazy) have populated
                    await EnsureSpecValuesReady(page);

                    // Ensure the gallery is ready so image attributes populate (even if images are blocked elsewhere)
                    await TryEnsureGalleryReady(page);

                    var sw = System.Diagnostics.Stopwatch.StartNew();
                    var row = await ScrapeVehicleAsync(page, navUrl);
                    AppendCsvRow(SpecsCsvPath, row);
                    sw.Stop();

                    doneNew++;
                    if (!string.IsNullOrEmpty(id)) alreadyScraped.Add(id);
                    LogScrapedCar(url: row[0], name: row[1], dealer: row[3], done: doneNew, total: toScrape.Count, elapsed: sw.Elapsed);
                }
                catch (TimeoutException tex)
                {
                    Console.WriteLine($"✖ Timeout on: {navUrl}\n   {tex.Message}");
                    AppendRemovedRow(RemovedCsvPath, id, navUrl, "Timeout", 0);
                    Console.WriteLine($"⚠️ Removed: {id} — Timeout.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✖ Failed: {navUrl}\n   {ex.Message}");
                    AppendRemovedRow(RemovedCsvPath, id, navUrl, "Exception", 0);
                    Console.WriteLine($"⚠️ Removed: {id} — Exception.");
                }
            }
        }
        finally
        {
            try { if (page is not null) await page.CloseAsync(); } catch { }
        }

        Console.WriteLine($"\n✅ Finished. New rows appended: {doneNew:N0}. Skipped (already scraped): {skipped:N0}.");
        Console.WriteLine($"📄 Specs CSV   → {SpecsCsvPath}");
        Console.WriteLine($"📄 URL CSV     → {UrlsCsvPath}");
        Console.WriteLine($"📄 Removed CSV → {RemovedCsvPath}");

        if (Headless == false)
        {
            Console.WriteLine("Browser left open. Press any key to close…");
            Console.ReadKey();
        }
        await ctx.CloseAsync();
        await browser.CloseAsync();
    }

    // --------------------- VEHICLE PACER ---------------------
    static async Task WaitVehicleNavSlotAsync()
    {
        int waitMs = 0;
        var now = DateTime.UtcNow;

        lock (_vehNavLock)
        {
            if (now < _vehicleBlockedUntilUtc)
            {
                waitMs = (int)Math.Ceiling((_vehicleBlockedUntilUtc - now).TotalMilliseconds);
            }
            else
            {
                var since = (int)(now - _lastVehicleNav).TotalMilliseconds;
                var need = VehicleMinNavGapMs - since;
                waitMs = need > 0 ? need : 0;
            }
            _lastVehicleNav = now.AddMilliseconds(waitMs + _rng.Next(VehicleJitterMinMs, VehicleJitterMaxMs));
        }

        if (waitMs > 0)
            await Task.Delay(waitMs + _rng.Next(VehicleJitterMinMs, VehicleJitterMaxMs));
    }

    static void NoteVehicle429Backoff(int waitMs)
    {
        var b = DateTime.UtcNow.AddMilliseconds(waitMs + _rng.Next(120, 320));
        lock (_vehNavLock)
        {
            if (b > _vehicleBlockedUntilUtc) _vehicleBlockedUntilUtc = b;
        }
    }

    // --------------------- URL COLLECTION FLOW ---------------------
    static async Task<List<string>> CollectUrlsFlow(IBrowserContext ctx)
    {
        var page = await ctx.NewPageAsync();

        await GoWithRetry(page, HomeUrl);
        await AcceptCookies(page);
        await page.WaitForTimeoutAsync(1000);

        var totalFromHome = await ReadHomeExpectedCountAsync(page, initialDelayMs: 0);
        if (totalFromHome > 0)
            Console.WriteLine($"ℹ️ Home reports ~{totalFromHome:N0} vehicles.");
        else
            Console.WriteLine("ℹ️ Couldn’t read total on home; will fall back to results.");

        await ClickSearchOnHome(page); // enters /result
        await AcceptCookies(page);

        string ResultsUrl(int pageNo, int size) =>
            $"{ResultsBase}?payment_type=cash&source=home&size={size}&page={pageNo}";

        await GoWithRetry(page, ResultsUrl(1, 96));
        await AcceptCookies(page);

        var allUrls = await CollectAllVehicleUrlsStrictAsync(
            page,
            pageSize: 96,
            buildUrl: p => ResultsUrl(p, 96),
            knownTotal: totalFromHome);

        Console.WriteLine($"\nCollected {allUrls.Count} unique vehicle URLs.");
        await page.CloseAsync();
        return allUrls;
    }

    // ---------- STRICT results harvesting ----------
    static async Task<List<string>> CollectAllVehicleUrlsStrictAsync(
        IPage page,
        int pageSize,
        Func<int, string> buildUrl,
        int knownTotal = 0)
    {
        int total = knownTotal;
        if (total <= 0)
        {
            total = await TryReadTotalResultsAsync(page);
            if (total > 0) Console.WriteLine($"ℹ️ Results page reports ~{total:N0} vehicles.");
        }

        int totalPages = total > 0 ? (int)Math.Ceiling(total / (double)pageSize) : 140; // safety cap
        var idToUrl = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (int pageNo = 1; pageNo <= totalPages; pageNo++)
        {
            int expectedHere = ExpectedOnPage(pageNo, pageSize, total);
            if (expectedHere == 0) expectedHere = pageSize;

            var pageEntries = await HarvestOnePageStrict(page, pageNo, expectedHere, pageSize, buildUrl);

            int added = 0;
            foreach (var (id, url) in pageEntries)
            {
                if (!string.IsNullOrEmpty(id) && !idToUrl.ContainsKey(id))
                {
                    idToUrl[id] = url;
                    added++;
                }
            }

            Console.WriteLine($"Page {pageNo}: collected {pageEntries.Count} ids; unique new {added}; total unique {idToUrl.Count}");

            if (_earlyStopRequested)
            {
                Console.WriteLine("🛑 Early-stop triggered — starting vehicle scraping with collected URLs.");
                break;
            }

            if (total > 0 && idToUrl.Count >= total)
            {
                Console.WriteLine("Reached reported total — stopping.");
                break;
            }

            await Task.Delay(_rng.Next(240, 520));
        }

        return idToUrl.Values.ToList();
    }

    static async Task<List<(string id, string url)>> HarvestOnePageStrict(
        IPage page,
        int pageNo,
        int expectedHere,
        int pageSize,
        Func<int, string> buildUrl,
        int stopAfterRetries = 5)
    {
        var best = new List<(string id, string url)>();
        int attempts = 0;
        int maxAttempts = 7;

        while (attempts++ < maxAttempts)
        {
            await RespectNavGap();
            var url = buildUrl(pageNo);
            var resp = await GoWithRetry(page, url);
            if (resp is null)
            {
                await Task.Delay(_rng.Next(PageSettleMinMs, PageSettleMaxMs));
                continue;
            }

            await AcceptCookies(page);
            await page.WaitForTimeoutAsync(_rng.Next(PageSettleMinMs, PageSettleMaxMs));
            await WaitForViewDetailsVisible(page, timeoutMs: 15000);
            await WaitForResultsShell(page);
            await WaitForCardCountToStabilize(page, minAim: Math.Min(24, pageSize / 4), maxWaitMs: 12000);
            await ProgressiveScroll(page);

            var entries = await CollectLinksFromViewDetails(page, pageSize);

            if (entries.Count > best.Count) best = entries;
            if (entries.Count >= expectedHere) return entries;

            Console.WriteLine($"  ↻ Page {pageNo}: got {entries.Count}/{expectedHere}, retrying…");

            if (attempts > stopAfterRetries)
            {
                _earlyStopRequested = true;
                Console.WriteLine($"🟡 Page {pageNo}: exceeded {stopAfterRetries} retries; accepting {best.Count}/{expectedHere} and stopping collection.");
                return best;
            }

            await Task.Delay(_rng.Next(PageSettleMinMs, PageSettleMaxMs));
            try { await page.ReloadAsync(new() { WaitUntil = WaitUntilState.DOMContentLoaded }); } catch { }
            await AcceptCookies(page);
            await WaitForResultsShell(page);
            await ProgressiveScroll(page);
        }

        if (best.Count < expectedHere)
            Console.WriteLine($"⚠️ Page {pageNo}: returning {best.Count}/{expectedHere} after {maxAttempts} attempts.");
        return best;
    }

    static int ExpectedOnPage(int pageNo, int pageSize, int total)
    {
        if (total <= 0) return 0;
        int totalPages = (int)Math.Ceiling(total / (double)pageSize);
        return pageNo < totalPages ? pageSize : Math.Max(0, total - pageSize * (totalPages - 1));
    }

    // ---------- Wait for View Details anchors ----------
    static async Task WaitForViewDetailsVisible(IPage page, int timeoutMs = 15000)
    {
        try
        {
            await page.WaitForFunctionAsync(
                @"() => {
                    const sel = 'a[data-tracking-linkid=""Details-View Details""], a.btn.btn-primary[href*=""/vehicle/""]';
                    const as = Array.from(document.querySelectorAll(sel));
                    return as.some(a => {
                        const href = a.getAttribute('href') || '';
                        const visible = a.offsetParent !== null;
                        return visible && /\/vehicle\//.test(href);
                    });
                }",
                new PageWaitForFunctionOptions { Timeout = timeoutMs }
            );
        }
        catch { }
    }

    // ---------- Collect strictly from "View details" anchors ----------
    static async Task<List<(string id, string url)>> CollectLinksFromViewDetails(IPage page, int pageSize)
    {
        int stableRounds = 0;
        int lastCount = -1;
        var best = new List<(string id, string url)>();

        for (int round = 0; round < 6; round++)
        {
            await ProgressiveScroll(page);

            var pairs = await page.EvaluateAsync<string[][]>(@"
                () => {
                  const re = /\/vehicle\/(\d+)/;
                  const sel = 'a[data-tracking-linkid=""Details-View Details""], a.btn.btn-primary[href*=""/vehicle/""]';
                  const map = new Map();
                  for (const a of Array.from(document.querySelectorAll(sel))) {
                    const h = a.getAttribute('href') || '';
                    const m = h.match(re);
                    if (!m) continue;
                    const id = m[1];
                    if (!map.has(id)) {
                      const abs = new URL(h, location.origin).toString();
                      map.set(id, abs);
                    }
                  }
                  return Array.from(map.entries()).map(([id, url]) => [id, url]);
                }");

            var current = pairs.Select(p => (id: p[0], url: p[1])).ToList();

            if (current.Count > best.Count) best = current;

            if (current.Count == lastCount) stableRounds++; else stableRounds = 0;
            lastCount = current.Count;

            if (current.Count >= pageSize) break;
            if (stableRounds >= 2) break;

            await Task.Delay(_rng.Next(180, 360));
        }

        return best;
    }

    // ---------- Page render helpers ----------
    static async Task WaitForCardCountToStabilize(IPage page, int minAim, int maxWaitMs)
    {
        var start = DateTime.UtcNow;
        int last = -1, stable = 0;

        while ((DateTime.UtcNow - start).TotalMilliseconds < maxWaitMs)
        {
            var count = await page.EvaluateAsync<int>(@"() => document.querySelectorAll('a[data-tracking-linkid=""Details-View Details""], a.btn.btn-primary[href*=""/vehicle/""]').length");

            if (count >= minAim) return;

            if (count == last) stable++; else stable = 0;
            last = count;

            if (stable >= 3) break;

            await ProgressiveScroll(page);
            await Task.Delay(_rng.Next(180, 360));
        }
    }

    static async Task ProgressiveScroll(IPage page)
    {
        try
        {
            await page.EvaluateAsync("() => window.scrollTo({ top: 0, behavior: 'instant' })");
            await page.WaitForTimeoutAsync(80);
            await page.EvaluateAsync("() => window.scrollTo({ top: document.body.scrollHeight * 0.6, behavior: 'instant' })");
            await page.WaitForTimeoutAsync(120);
            await page.EvaluateAsync("() => window.scrollTo({ top: document.body.scrollHeight, behavior: 'instant' })");
            await page.WaitForTimeoutAsync(180);
        }
        catch { }
    }

    static async Task WaitForResultsShell(IPage page)
    {
        try
        {
            await page.WaitForFunctionAsync(@"() => {
                return document.querySelector('.uvl-c-results') ||
                       document.querySelector('.uvl-c-vehicle-card') ||
                       document.querySelector('a[data-tracking-linkid=""Details-View Details""]') ||
                       document.querySelector('a.btn.btn-primary[href*=""/vehicle/""]');
            }", new PageWaitForFunctionOptions { Timeout = 15000 });
        }
        catch { }
    }

    // ---------- TOTALS (HOME / fallback) ----------
    static async Task<int> ReadHomeExpectedCountAsync(IPage page, int initialDelayMs = 1000)
    {
        try
        {
            if (initialDelayMs > 0)
                await page.WaitForTimeoutAsync(initialDelayMs);

            var counter = page.Locator(".uvl-c-expected-results-btn__label__counter");
            if (await counter.CountAsync() > 0)
            {
                await counter.First.WaitForAsync(new() { Timeout = 12000 });
                var txt = await counter.First.InnerTextAsync();
                var raw = (txt ?? string.Empty).Replace(",", "").Replace(".", "").Trim();
                if (int.TryParse(raw, out var n) && n > 0) return n;
            }

            var btn = page.Locator("button.uvl-c-expected-results-btn");
            if (await btn.CountAsync() > 0)
            {
                await btn.First.WaitForAsync(new() { Timeout = 12000 });
                string txt = await btn.First.InnerTextAsync();
                var m = Regex.Match(txt ?? "", @"Search\s+([\d,\.]+)\s+vehicles", RegexOptions.IgnoreCase);
                if (m.Success)
                {
                    var raw = m.Groups[1].Value.Replace(",", "").Replace(".", "");
                    if (int.TryParse(raw, out var n)) return n;
                }
            }

            var body = await page.InnerTextAsync("body");
            int maxVal = 0;
            foreach (Match m in Regex.Matches(body ?? "", @"Search\s+([\d,\.]+)\s+vehicles", RegexOptions.IgnoreCase))
            {
                var raw = m.Groups[1].Value.Replace(",", "").Replace(".", "");
                if (int.TryParse(raw, out var n)) maxVal = Math.Max(maxVal, n);
            }
            return maxVal;
        }
        catch { return 0; }
    }

    static async Task<int> TryReadTotalResultsAsync(IPage page)
    {
        try
        {
            var body = await page.InnerTextAsync("body");
            int maxVal = 0;
            foreach (Match m in Regex.Matches(body ?? "", @"Search\s+([\d,\.]+)\s+vehicles", RegexOptions.IgnoreCase))
            {
                var raw = m.Groups[1].Value.Replace(",", "").Replace(".", "");
                if (int.TryParse(raw, out var n)) maxVal = Math.Max(maxVal, n);
            }
            return maxVal;
        }
        catch { return 0; }
    }

    static async Task ClickSearchOnHome(IPage page)
    {
        try
        {
            var btn = page.Locator("button.uvl-c-expected-results-btn");
            if (await btn.IsVisibleAsync())
            {
                await btn.ScrollIntoViewIfNeededAsync();
                await btn.ClickAsync(new() { Timeout = 10000 });
            }
            else
            {
                var alt = page.GetByRole(AriaRole.Button, new()
                {
                    NameRegex = new Regex(@"Search\s+\d[\d,\.]*\s+vehicles", RegexOptions.IgnoreCase)
                });
                if (await alt.IsVisibleAsync()) await alt.ClickAsync(new() { Timeout = 10000 });
            }

            try { await page.WaitForURLAsync("**/result/**", new() { Timeout = 15000 }); } catch { }
            await WaitForResultsShell(page);
        }
        catch { }
    }

   // ---------- Vehicle page scraping ----------
static async Task<string[]> ScrapeVehicleAsync(IPage page, string url)
{
    // UPDATED: use robust title grabber
    var name = await GetVehicleTitleAsync(page);

    var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    // OVERVIEW
    var overviewRoot = page.Locator(".uvl-c-specification-overview");
    try
    {
        await overviewRoot.WaitForAsync(new() { Timeout = 8000 });
        var titles = overviewRoot.Locator(".uvl-c-specification-overview__title");
        int n = await titles.CountAsync();
        for (int i = 0; i < n; i++)
        {
            var t = titles.Nth(i);
            var label = Clean(await t.Locator("span").InnerTextAsync());
            var valueEl = t.Locator("xpath=following-sibling::*[contains(@class,'uvl-c-specification-overview__value')][1]");
            string value = (await valueEl.CountAsync() > 0) ? Clean(await valueEl.InnerTextAsync()) : "";
            if (!string.IsNullOrEmpty(label)) dict[label] = value;
        }
    } catch {}

    // DETAILED SPECS (3 columns; can be lazy)
    await EnsureSpecValuesReady(page);

    var spec = page.Locator(".uvl-c-specification-details li.uvl-c-stat");
    try { await page.Locator(".uvl-c-specification-details").WaitForAsync(new() { Timeout = 8000 }); } catch {}
    int sc = await spec.CountAsync();
    for (int i = 0; i < sc; i++)
    {
        var r = spec.Nth(i);
        var label = Clean(await r.Locator(".uvl-c-stat__title").InnerTextAsync());
        var value = Clean(await r.Locator(".uvl-c-stat__value").InnerTextAsync());
        if (!string.IsNullOrEmpty(label) && !dict.ContainsKey(label)) dict[label] = value;
    }

    var imgUrl = await GetHeroImageUrl(page);

    var dealer = await GetDealerNumberFastAsync(page);
    var model  = await GetModelCodeFastAsync(page);

    // ----------- SANITIZE FIELDS FOR IMPORTER -----------
    string mileage             = ExtractNumber(dict.GetValueOrDefault("Mileage", ""), allowDecimal:false);
    string registeredIso       = NormalizeRegisteredToIso(dict.GetValueOrDefault("Registered", ""));
    string exterior            = dict.GetValueOrDefault("Exterior","");
    string transmission        = dict.GetValueOrDefault("Transmission","");
    string registrationPlate   = Regex.Replace(dict.GetValueOrDefault("Registration",""), @"\s+", "");

    // Specs
    string fuel                = dict.GetValueOrDefault("Fuel","");
    string engineSizeCc        = ExtractNumber(dict.GetValueOrDefault("Engine size",""), false);
    string cylinders           = ExtractNumber(dict.GetValueOrDefault("Cylinders",""), false);
    string power               = ExtractNumber(dict.GetValueOrDefault("Power",""), false);
    string topSpeedMph         = ExtractNumber(dict.GetValueOrDefault("Top speed",""), false);
    string torqueNm            = ExtractNumber(dict.GetValueOrDefault("Torque",""), false);
    string drivetrain          = dict.GetValueOrDefault("Drivetrain","");

    // NEW electric/extra specs
    string accelerationSec        = ExtractNumber(dict.GetValueOrDefault("Acceleration",""), true);
    string energyConsumptionMpkWh = ExtractNumber(dict.GetValueOrDefault("Energy consumption",""), true);
    string electricRangeMiles     = ExtractNumber(dict.GetValueOrDefault("Electric range",""), false);
    string electricRangeCityMiles = ExtractNumber(dict.GetValueOrDefault("Electric range city",""), false);

    string mpgCombined         = ExtractNumber(dict.GetValueOrDefault("Fuel consumption medium",""), true);
    string co2Wltp             = ExtractNumber(dict.GetValueOrDefault("CO2 emissions (WLTP)", ""), allowDecimal:false);
    string weightKg            = ExtractNumber(dict.GetValueOrDefault("Weight",""), false);
    string bootLitres          = ExtractNumber(dict.GetValueOrDefault("Boot volume seats up",""), false);

    // NEW dimensions/insurance
    string heightMm            = ExtractNumber(dict.GetValueOrDefault("Height",""), false);
    string widthMm             = ExtractNumber(dict.GetValueOrDefault("Width",""), false);
    string lengthMm            = ExtractNumber(dict.GetValueOrDefault("Length",""), false);
    string insuranceCategory   = dict.GetValueOrDefault("Insurance category","");

    // NEW: Retail price ("Pay in full")
    string retailPrice         = ExtractNumber(await GetRetailPriceTextAsync(page), allowDecimal:false);

    // New requested fields
    string colour              = BasicColour(exterior); // generalised colour
    string brand               = "BMW";
    string bodyStyle           = await GetBodyStyleAsync(page, model, name);

    // Build row (order must match Header)
    var row = new[]
    {
        url, name, model, dealer,
        retailPrice,
        mileage, registeredIso, exterior, transmission, registrationPlate,
        fuel, engineSizeCc, cylinders, power,
        topSpeedMph, torqueNm, drivetrain,
        accelerationSec,
        energyConsumptionMpkWh,
        electricRangeMiles,
        electricRangeCityMiles,
        mpgCombined,
        co2Wltp,
        weightKg, bootLitres,
        heightMm, widthMm, lengthMm, insuranceCategory,
        imgUrl,
        bodyStyle, colour, brand,
        ""
    };

    return row;
}

// ---------- CSV (vehicle spec output) ----------
static string[] Header() => new[]
{
    "VehicleWebsiteLink","Name","ModelCode","DealerNumber",
    "RetailPrice",
    "Mileage","Registered","Exterior","Transmission","Registration",
    "Fuel","Engine size","Cylinders","Power",
    "Top speed","Torque","Drivetrain",
    "Acceleration (0-62 mph, Sec)",
    "Energy consumption (miles/kWh)",
    "Electric range (miles)",
    "Electric range city (miles)",
    "Fuel consumption medium",
    "CO2 (WLTP)",
    "Weight","Boot volume seats up",
    "Height (mm)","Width (mm)","Length (mm)","Insurance category",
    "Images[0].Url",
    "BodyStyle","Colour","Brand",
    "WorkspaceOrganisationReference"
};

    static void EnsureSpecsCsvHeader(string path)
    {
        var dir = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(dir);

        var expectedHeaderLine = string.Join(",", Header().Select(Csv)) + "\n";

        if (!File.Exists(path))
        {
            File.WriteAllText(path, expectedHeaderLine, Encoding.UTF8);
            return;
        }

        try
        {
            var first = File.ReadLines(path, Encoding.UTF8).FirstOrDefault() ?? "";
            var have = ParseCsvLine(first);
            var want = Header();

            bool mismatch = have.Length != want.Length ||
                            !have.SequenceEqual(want, StringComparer.OrdinalIgnoreCase);

            if (mismatch)
            {
                var bak = path.Replace(".csv", $".legacy.{DateTime.UtcNow:yyyyMMddHHmmss}.csv");
                File.Move(path, bak, overwrite: false);
                File.WriteAllText(path, expectedHeaderLine, Encoding.UTF8);
                Console.WriteLine($"🆕 Specs CSV header updated. Old file moved to: {bak}");
            }
        }
        catch
        {
            File.WriteAllText(path, expectedHeaderLine, Encoding.UTF8);
        }
    }

    static readonly object _fileLock = new();
    static readonly object _removedLock = new(); // separate lock for removed csv

    static void AppendCsvRow(string path, string[] values)
    {
        var line = string.Join(",", values.Select(Csv)) + "\n";
        lock (_fileLock) { File.AppendAllText(path, line, Encoding.UTF8); }
    }

    // ---------- Robust cookie handling ----------
    static bool _cookiesAccepted = false; // remember once per context

    static async Task AcceptCookies(IPage page, int maxRounds = 4)
    {
        if (_cookiesAccepted) return;

        for (int round = 0; round < maxRounds; round++)
        {
            bool clicked = await TryAcceptCookiesInAllFrames(page);
            if (clicked)
            {
                await page.WaitForTimeoutAsync(200);
                bool stillThere = await IsAnyCookieBannerVisible(page);
                if (!stillThere) { _cookiesAccepted = true; return; }
            }
            await page.WaitForTimeoutAsync(150);
        }
    }

    static async Task<bool> TryAcceptCookiesInAllFrames(IPage page)
    {
        var frames = new List<IFrame> { page.MainFrame };
        frames.AddRange(page.Frames.Where(f => f != page.MainFrame));

        foreach (var frame in frames)
        {
            try
            {
                string[] selectors =
                {
                    "#onetrust-accept-btn-handler",
                    "button#onetrust-accept-btn-handler",
                    "button[aria-label*='Accept' i]",
                    "button[title*='Accept' i]",
                    "button.accept-button:has-text('Accept all')",
                    "button:has-text('Accept all cookies')",
                    "button:has-text('Accept all')",
                    "button:has-text('Allow all')",
                    "button:has-text('I agree')",
                    "button:has-text('Agree and proceed')",
                    "button:has-text('Got it')"
                };

                foreach (var sel in selectors)
                {
                    var loc = frame.Locator(sel);
                    if (await loc.CountAsync() > 0 && await loc.First.IsVisibleAsync())
                    {
                        try
                        {
                            await loc.First.ClickAsync(new() { Force = true });
                            return true;
                        }
                        catch { }
                    }
                }

                var role = frame.GetByRole(AriaRole.Button, new()
                {
                    NameRegex = new Regex("(Accept|Allow).*(all|cookies)|I agree|Agree and proceed|Got it", RegexOptions.IgnoreCase)
                });
                if (await role.CountAsync() > 0 && await role.First.IsVisibleAsync())
                {
                    try
                    {
                        await role.First.ClickAsync(new() { Force = true });
                        return true;
                    }
                    catch { }
                }
            }
            catch { }
        }
        return false;
    }

    static async Task<bool> IsAnyCookieBannerVisible(IPage page)
    {
        try
        {
            string script = @"() => {
              const qs = [
                '#onetrust-banner-sdk',
                '.onetrust-pc-dark-filter',
                '.ot-sdk-container',
                '[id*=""cookie""][role=""dialog""]',
                '[class*=""cookie""][role=""dialog""]'
              ];
              return qs.some(q => {
                const el = document.querySelector(q);
                return el && window.getComputedStyle(el).display !== 'none' && el.offsetParent !== null;
              });
            }";
            return await page.EvaluateAsync<bool>(script);
        }
        catch { return false; }
    }

    // ---------- Generic helpers ----------
    static async Task RespectNavGap()
    {
        int wait = 0;
        lock (_navLock)
        {
            var since = (int)(DateTime.UtcNow - _lastNav).TotalMilliseconds;
            var need = MinNavGapMs - since;
            wait = need > 0 ? need : 0;
            _lastNav = DateTime.UtcNow.AddMilliseconds(Math.Max(wait, 0));
        }
        if (wait > 0) await Task.Delay(wait + _rng.Next(80, 260));
    }

    static async Task<IResponse?> GoWithRetry(IPage page, string url, int maxAttempts = 5, bool throttle = true, bool isVehicle = false, bool quiet429 = false)
    {
        for (int attempt = 1; attempt <= maxAttempts; attempt++)
        {
            if (!isVehicle)
            {
                if (throttle) await RespectNavGap();
            }

            var resp = await page.GotoAsync(url, new()
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout   = isVehicle ? 45000 : 30000
            });

            var status = resp?.Status ?? 0;

            if (status >= 400 && status != 429)
                Console.WriteLine($"HTTP {status} on {url} (attempt {attempt}/{maxAttempts})");

            if (status != 429 && status != 0) return resp;

            int waitMs = GetRetryAfterMs(resp);
            if (waitMs <= 0)
                waitMs = (int)Math.Min(3000, Math.Pow(2, attempt) * 400) + _rng.Next(100, 300);

            if (isVehicle) NoteVehicle429Backoff(waitMs);

            if (!quiet429 || attempt >= 3)
                Console.WriteLine($"⚠️ 429 on {url} (attempt {attempt}/{maxAttempts}). Waiting {waitMs}ms…");

            await Task.Delay(waitMs);
        }
        return null;
    }

    static int GetRetryAfterMs(IResponse? resp)
    {
        if (resp?.Headers == null) return 0;
        if (resp.Headers.TryGetValue("Retry-After", out var ra) || resp.Headers.TryGetValue("retry-after", out ra))
        {
            if (int.TryParse(ra, out var secs))
                return Math.Clamp(secs * 1000, 1000, 20000);
        }
        return 0;
    }

    static async Task<bool> EnsureVehicleDom(IPage page, int timeoutMs = 8000)
    {
        var start = DateTime.UtcNow;
        while ((DateTime.UtcNow - start).TotalMilliseconds < timeoutMs)
        {
            try
            {
                var ok = await page.EvaluateAsync<bool>(@"() => {
                    const title = document.querySelector('h1, .uvl-c-vehicle-header__title h1, .vehicle-title h1');
                    const hasTitle = !!title && (title.textContent || '').trim().length > 0;
                    const spec = document.querySelector('.uvl-c-specification-overview') || document.querySelector('.uvl-c-specification-details');
                    return hasTitle || !!spec;
                }");
                if (ok) return true;
            }
            catch { }
            await Task.Delay(150);
        }
        return false;
    }

    // Make sure spec values have actually been filled in
    static async Task EnsureSpecValuesReady(IPage page, int minNonEmpty = 10, int timeoutMs = 3500)
    {
        var start = DateTime.UtcNow;

        try
        {
            var details = page.Locator(".uvl-c-specification-details");
            if (await details.CountAsync() > 0)
                await details.First.ScrollIntoViewIfNeededAsync();
        }
        catch { }

        while ((DateTime.UtcNow - start).TotalMilliseconds < timeoutMs)
        {
            try
            {
                await page.EvaluateAsync(@"() => window.scrollBy({ top: Math.round(window.innerHeight * 0.6), left: 0, behavior: 'instant' })");
            }
            catch { }

            try
            {
                int filled = await page.EvaluateAsync<int>(@"() =>
                    Array.from(document.querySelectorAll('.uvl-c-stat__value'))
                         .filter(v => (v.textContent || '').trim().length > 0).length
                ");
                if (filled >= minNonEmpty) break;
            }
            catch { }

            await page.WaitForTimeoutAsync(140);
        }
    }

    // UPDATED: robustly get the vehicle title, preferring the aria-label
    static async Task<string> GetVehicleTitleAsync(IPage page)
    {
        var h1 = page.Locator("h1.uvl-c-vehicle-identifier__title, .uvl-c-vehicle-header__title h1, .vehicle-title h1, h1");
        if (await h1.CountAsync() == 0) return "";
        var first = h1.First;

        try
        {
            var aria = await first.GetAttributeAsync("aria-label");
            if (!string.IsNullOrWhiteSpace(aria))
            {
                var m = Regex.Match(aria, @"Vehicle title:\s*(.+)$", RegexOptions.IgnoreCase);
                if (m.Success) return Clean(m.Groups[1].Value);
                return Clean(aria);
            }
        }
        catch { }

        try { return Clean(await first.InnerTextAsync()); } catch { return ""; }
    }

    static async Task<string> FirstText(IPage page, string selector)
    {
        try { var loc = page.Locator(selector); if (await loc.CountAsync() == 0) return ""; return Clean(await loc.First.InnerTextAsync()); }
        catch { return ""; }
    }

    // NEW: get the retail price text (prefers the "Pay in full" card)
    static async Task<string> GetRetailPriceTextAsync(IPage page)
    {
        try
        {
            var txt = await page.EvaluateAsync<string>(@"
              () => {
                const get = v => (v || '').trim();
                // Look for a block whose title indicates 'Pay in full'
                const roots = Array.from(document.querySelectorAll('[class*=""vehicle-quote""]'));
                for (const r of roots) {
                  const title = get(r.querySelector('.uvl-c-vehicle-quote__title, .uvl-c-vehicle-quote__label')?.textContent).toLowerCase();
                  const fig = r.querySelector('.uvl-c-vehicle-quote__figure');
                  if (title.includes('pay in full') && fig) return get(fig.textContent);
                }
                // Fallback: first visible figure
                const f = document.querySelector('.uvl-c-vehicle-quote__figure');
                return f ? get(f.textContent) : '';
              }");
            return txt ?? "";
        }
        catch { return ""; }
    }

    static string Clean(string s) => string.IsNullOrWhiteSpace(s) ? "" : Regex.Replace(s, @"\s+", " ").Trim();

    static string Csv(string s)
    {
        if (s is null) return "";
        s = s.Replace("\"", "\"\"");
        return (s.Contains(',', StringComparison.Ordinal) || s.Contains('"') || s.Contains('\n')) ? $"\"{s}\"" : s;
    }

    // Ensure image attributes populate for lazy loaders
    static async Task TryEnsureGalleryReady(IPage page)
    {
        if (!AllowVehicleImages) return;

        try
        {
            var gal = page.Locator(".uvl-c-swiper, .uvl-c-swiper__image");
            if (await gal.CountAsync() > 0)
                await gal.First.ScrollIntoViewIfNeededAsync();

            try
            {
                await page.WaitForFunctionAsync(@"() => {
                  const img = document.querySelector('img.uvl-c-swiper__image');
                  if (!img) return true;
                  const a = img.getAttribute('srcset') || img.getAttribute('src') || img.getAttribute('data-srcset') || img.getAttribute('data-original');
                  return !!a;
                }", new PageWaitForFunctionOptions { Timeout = 500 });
            }
            catch { }
        }
        catch { }
    }

    // Vehicle-page utilities
    static async Task<string> GetDealerNumberFastAsync(IPage page)
    {
        var dn = await page.EvaluateAsync<string>(@"() => {
            try {
              const rs = (window.UVL?.ADVERT?.retailer_site) || (window.UVL?.ADVERT?.retailerSite);
              if (rs && (rs.dealer_number || rs.dealerNumber)) return String(rs.dealer_number || rs.dealerNumber);
              const hf = window.UVL?.FORM?.hiddenFields || [];
              const f = Array.isArray(hf) ? hf.find(x => x?.name === 'retailerId') : null;
              if (f) return String(f.value || f.val || '');
            } catch {}
            return '';
        }");
        if (!string.IsNullOrWhiteSpace(dn)) return dn.Trim();

        var scripts = page.Locator("script:not([src])");
        int n = await scripts.CountAsync();
        for (int i = 0; i < n; i++)
        {
            var text = await scripts.Nth(i).InnerTextAsync();
            var m = Regex.Match(text ?? "", @"name\s*:\s*['""]retailerId['""]\s*,\s*value\s*:\s*['""](?<val>\d{3,})['""]");
            if (m.Success) return m.Groups["val"].Value;
        }
        return "";
    }

    static async Task<string> GetModelCodeFastAsync(IPage page)
    {
        var code = await page.EvaluateAsync<string>(@"() => {
  try {
    const ok = v => typeof v === 'string' && v.trim().length > 0;
    const mc = (window.UVL?.ADVERT?.model_code) || (window.UVL?.ADVERT?.modelCode);
    if (ok(mc)) return String(mc).trim();
    const hf = window.UVL?.FORM?.hiddenFields;
    if (Array.isArray(hf)) {
      const f = hf.find(x => x && x.name === 'moModel' && ok(String(x.value ?? x.val ?? '')));
      if (f) return String(f.value ?? f.val ?? '').trim();
    }
    const re1 = /model_code['""]\s*:\s*['""]([A-Za-z0-9_-]+)['""]/;
    const re2 = /name\s*:\s*['""]moModel['""]\s*,\s*value\s*:\s*['""]([A-Za-z0-9_-]+)['""]/;
    for (const s of Array.from(document.scripts)) {
      try {
        const t = s.textContent || '';
        let m = t.match(re1); if (m) return m[1];
        m = t.match(re2); if (m) return m[1];
      } catch(e) {}
    }
  } catch(e) {}
  return '';
}");
        return code?.Trim() ?? "";
    }

    static async Task<string> GetHeroImageUrl(IPage page)
    {
        // 1) Open Graph image
        try
        {
            var og = page.Locator("meta[property='og:image']");
            if (await og.CountAsync() > 0)
            {
                var ogUrl = await og.First.GetAttributeAsync("content");
                if (!string.IsNullOrWhiteSpace(ogUrl)) return ogUrl;
            }
        }
        catch { }

        // 2) <picture><source srcset=...> pick largest
        try
        {
            var fromPicture = await page.EvaluateAsync<string>(@"
              () => {
                const pic = document.querySelector('.uvl-c-swiper picture');
                if (!pic) return '';
                let bestW = -1, best = '';
                for (const s of pic.querySelectorAll('source[srcset]')) {
                  const parts = (s.getAttribute('srcset')||'').split(',');
                  for (const part of parts) {
                    const m = part.trim().match(/(\S+)\s+(\d+)w/);
                    if (m) {
                      const w = parseInt(m[2],10);
                      if (w > bestW) { bestW = w; best = m[1]; }
                    }
                  }
                }
                return best;
              }");
            if (!string.IsNullOrWhiteSpace(fromPicture)) return fromPicture;
        }
        catch { }

        // 3) Primary <img> in the gallery (with multiple fallbacks)
        try
        {
            var img = page.Locator("img.uvl-c-swiper__image");
            if (await img.CountAsync() > 0)
            {
                var srcset = await img.First.GetAttributeAsync("srcset")
                             ?? await img.First.GetAttributeAsync("data-srcset");

                var best = PickLargestFromSrcSet(srcset);
                if (!string.IsNullOrWhiteSpace(best)) return best;

                var src = await img.First.GetAttributeAsync("src")
                          ?? await img.First.GetAttributeAsync("data-src");
                if (!string.IsNullOrWhiteSpace(src)) return src;

                var original = await img.First.GetAttributeAsync("data-original");
                if (!string.IsNullOrWhiteSpace(original))
                    return original.Replace("{resize}", "w1280");
            }
        }
        catch { }

        // 4) JSON-LD schema.org image
        try
        {
            var blobs = await page.EvaluateAsync<string[]>(@"
              () => Array.from(document.querySelectorAll('script[type=""application/ld+json""]'))
                          .map(s => s.textContent || '')
            ");
            foreach (var raw in blobs)
            {
                if (string.IsNullOrWhiteSpace(raw)) continue;
                try
                {
                    using var doc = JsonDocument.Parse(raw);
                    var root = doc.RootElement;
                    if (root.ValueKind == JsonValueKind.Object && root.TryGetProperty("image", out var imageProp))
                    {
                        if (imageProp.ValueKind == JsonValueKind.String)
                            return imageProp.GetString() ?? "";
                        if (imageProp.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var it in imageProp.EnumerateArray())
                                if (it.ValueKind == JsonValueKind.String)
                                    return it.GetString() ?? "";
                        }
                    }
                }
                catch { /* ignore bad JSON-LD */ }
            }
        }
        catch { }

        return "";
    }

    static string PickLargestFromSrcSet(string? srcset)
    {
        if (string.IsNullOrWhiteSpace(srcset)) return "";
        int bestW = -1; string best = "";
        foreach (var part in srcset.Split(','))
        {
            var p = part.Trim();
            var m = Regex.Match(p, @"^(?<url>\S+)\s+(?<w>\d+)w$");
            if (!m.Success) continue;
            int w = int.Parse(m.Groups["w"].Value);
            if (w > bestW) { bestW = w; best = m.Groups["url"].Value; }
        }
        return best;
    }

    // ---------- URL normalizer ----------
    static string NormalizeVehicleUrl(string u)
    {
        if (string.IsNullOrWhiteSpace(u)) return u;
        try
        {
            var uri = new Uri(u);
            if (!uri.AbsolutePath.Contains("/vehicle/")) return u;
            return $"{uri.Scheme}://{uri.Host}{uri.AbsolutePath}";
        }
        catch { return u; }
    }

    // ---------- URL CSV (fresh every run) ----------
    static void SaveUrlsCsvFresh(string path, IEnumerable<string> urls)
    {
        var dir = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(dir);

        var distinct = urls
            .Where(u => !string.IsNullOrWhiteSpace(u))
            .Select(u => u.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var sb = new StringBuilder();
        sb.AppendLine("VehicleId,Url,CollectedAtUtc");
        string now = DateTime.UtcNow.ToString("o");
        foreach (var url in distinct)
        {
            var id = ExtractVehicleId(url);
            sb.AppendLine($"{Csv(id)},{Csv(url)},{Csv(now)}");
        }
        File.WriteAllText(path, sb.ToString(), Encoding.UTF8);
    }

    static List<string> LoadUrlsCsv(string path)
    {
        var urls = new List<string>();
        if (!File.Exists(path)) return urls;

        try
        {
            using var sr = new StreamReader(path, Encoding.UTF8);
            string? header = sr.ReadLine(); // skip header if present
            string? line;
            while ((line = sr.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var fields = ParseCsvLine(line);
                if (fields.Length == 0) continue;

                // Try to infer whether file has header VehicleId,Url,...
                // If first field looks like a URL, use it; else assume second field is URL.
                if (fields.Length >= 2 && !fields[0].StartsWith("http", StringComparison.OrdinalIgnoreCase))
                {
                    var u = fields[1]?.Trim();
                    if (!string.IsNullOrWhiteSpace(u)) urls.Add(u);
                }
                else
                {
                    var u = fields[0]?.Trim();
                    if (!string.IsNullOrWhiteSpace(u)) urls.Add(u);
                }
            }
        }
        catch { }

        return urls.Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    // ---------- Specs CSV -> loaded scraped IDs (by VehicleId from URL) ----------
    static HashSet<string> LoadScrapedIdsFromSpecs(string path)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (!File.Exists(path)) return set;

        try
        {
            using var sr = new StreamReader(path, Encoding.UTF8);
            string? header = sr.ReadLine(); // header
            string? line;
            while ((line = sr.ReadLine()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;
                var fields = ParseCsvLine(line);
                if (fields.Length == 0) continue;
                var link = fields[0]; // VehicleWebsiteLink
                if (string.IsNullOrWhiteSpace(link)) continue;
                var id = ExtractVehicleId(link);
                if (!string.IsNullOrEmpty(id)) set.Add(id);
            }
        }
        catch { }
        return set;
    }

    static string ExtractVehicleId(string url)
    {
        var m = Regex.Match(url ?? "", @"/vehicle/(\d+)", RegexOptions.IgnoreCase);
        return m.Success ? m.Groups[1].Value : "";
    }

    static string[] ParseCsvLine(string line)
    {
        var fields = new List<string>();
        var sb = new StringBuilder();
        bool inQuotes = false;

        for (int i = 0; i < line.Length; i++)
        {
            char c = line[i];
            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
                    else inQuotes = false;
                }
                else sb.Append(c);
            }
            else
            {
                if (c == ',') { fields.Add(sb.ToString()); sb.Clear(); }
                else if (c == '"') inQuotes = true;
                else sb.Append(c);
            }
        }
        fields.Add(sb.ToString());
        return fields.ToArray();
    }

    // ==== NEW HELPERS: build live IDs and prune the Specs CSV ====
    static HashSet<string> BuildLiveIdsFromUrls(IEnumerable<string> urls)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (urls == null) return set;

        foreach (var raw in urls)
        {
            if (string.IsNullOrWhiteSpace(raw)) continue;
            var id = ExtractVehicleId(NormalizeVehicleUrl(raw));
            if (!string.IsNullOrEmpty(id)) set.Add(id);
        }
        return set;
    }

    static int PruneSpecsCsvByLiveIds(string specsPath, HashSet<string> liveIds)
    {
        if (!File.Exists(specsPath)) return 0;
        if (liveIds == null || liveIds.Count == 0)
        {
            Console.WriteLine("⚠️  Prune skipped: live ID set is empty.");
            return 0;
        }

        var lines = File.ReadAllLines(specsPath, Encoding.UTF8);
        if (lines.Length == 0) return 0;

        var header = lines[0];
        var kept = new List<string>(capacity: lines.Length) { header };
        int removed = 0;

        for (int i = 1; i < lines.Length; i++)
        {
            var line = lines[i];
            if (string.IsNullOrWhiteSpace(line)) continue;

            string[] fields;
            try { fields = ParseCsvLine(line); }
            catch { kept.Add(line); continue; }

            if (fields.Length == 0) { kept.Add(line); continue; }

            var link = fields[0]; // VehicleWebsiteLink
            if (string.IsNullOrWhiteSpace(link)) { kept.Add(line); continue; }

            var id = ExtractVehicleId(link);
            if (!string.IsNullOrEmpty(id) && liveIds.Contains(id))
                kept.Add(line);
            else
                removed++;
        }

        var tmp = specsPath + ".tmp";
        var bak = specsPath + ".bak";

        File.WriteAllLines(tmp, kept, Encoding.UTF8);

        try { if (File.Exists(bak)) File.Delete(bak); } catch { /* ignore */ }
        try
        {
            if (File.Exists(specsPath))
                File.Move(specsPath, bak);
        }
        catch { /* ignore backup failure */ }

        if (File.Exists(specsPath)) File.Delete(specsPath);
        File.Move(tmp, specsPath);

        return removed;
    }
    // ==== END NEW HELPERS ====

    // ==== REMOVED CARS CSV + SOFT-404 DETECTION ====
    static void EnsureRemovedCsvHeader(string path)
    {
        var dir = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(dir);

        if (!File.Exists(path))
        {
            File.WriteAllText(path, "VehicleId,Url,Reason,Status,SeenAtUtc\n", Encoding.UTF8);
        }
        else
        {
            var fi = new FileInfo(path);
            if (fi.Length == 0)
                File.WriteAllText(path, "VehicleId,Url,Reason,Status,SeenAtUtc\n", Encoding.UTF8);
        }
    }

    static void AppendRemovedRow(string path, string id, string url, string reason, int status)
    {
        var now = DateTime.UtcNow.ToString("o");
        var line = $"{Csv(id)},{Csv(url)},{Csv(reason)},{status},{Csv(now)}\n";
        lock (_removedLock) { File.AppendAllText(path, line, Encoding.UTF8); }
    }

    static async Task<bool> IsSoft404Async(IPage page)
    {
        try
        {
            return await page.EvaluateAsync<bool>(@"
              () => {
                const txt = (document.body?.innerText || '').toLowerCase();
                const needles = [
                  'sorry, this page cannot be found',
                  'this page cannot be found',
                  'page not found'
                ];
                return needles.some(n => txt.includes(n));
              }");
        }
        catch { return false; }
    }
    // ==== END NEW ====

    static RunMode? ParseMode(string[] args)
    {
        foreach (var a in args ?? Array.Empty<string>())
        {
            if (!a.StartsWith("--mode=", StringComparison.OrdinalIgnoreCase)) continue;
            var v = a.Substring("--mode=".Length).Trim().ToLowerInvariant();
            return v switch
            {
                "auto" => RunMode.Auto,
                "scrape-from-csv-only" => RunMode.ScrapeFromCsvOnly,
                "collect-then-scrape" => RunMode.CollectThenScrape,
                "collect-only" => RunMode.CollectOnly,
                _ => null
            };
        }
        return null;
    }

    static bool HasArg(string[] args, string flag)
        => args.Any(a => string.Equals(a, flag, StringComparison.OrdinalIgnoreCase));

    // ---------- Logging ----------
    static void LogScrapedCar(string url, string name, string dealer, int done, int total, TimeSpan elapsed)
    {
        var id = ExtractVehicleId(url);
        var title = string.IsNullOrWhiteSpace(name) ? "(no title)" : name;
        var dealerShort = string.IsNullOrWhiteSpace(dealer) ? "—" : $"Dealer {dealer}";
        Console.WriteLine($"✅ [{done}/{total}] {id} — {title} ({dealerShort}) in {elapsed.TotalSeconds:0.0}s");
    }

    // ---------- Sanitizers ----------
    static string ExtractNumber(string? s, bool allowDecimal)
    {
        if (string.IsNullOrWhiteSpace(s)) return "";
        s = s.Replace(",", ""); // remove thousands separators
        var m = Regex.Match(s, allowDecimal ? @"\d+(\.\d+)?" : @"\d+");
        return m.Success ? m.Value : "";
    }

    static string NormalizeRegisteredToIso(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "";
        raw = raw.Trim();

        var enGB = CultureInfo.GetCultureInfo("en-GB");
        // Month Year -> first of month
        if (Regex.IsMatch(raw, @"^[A-Za-z]{3,9}\s+\d{4}$"))
        {
            if (DateTime.TryParseExact(raw, new[] { "MMM yyyy", "MMMM yyyy" }, enGB, DateTimeStyles.None, out var dt))
                return new DateTime(dt.Year, dt.Month, 1).ToString("yyyy-MM-dd");
        }

        // Common explicit formats
        string[] fmts = { "yyyy-MM-dd", "dd/MM/yyyy", "d/M/yyyy", "yyyy/MM/dd", "dd-MM-yyyy", "d-M-yyyy" };
        if (DateTime.TryParseExact(raw, fmts, enGB, DateTimeStyles.None, out var d1))
            return d1.ToString("yyyy-MM-dd");

        // Last resort: free parse, then normalize
        if (DateTime.TryParse(raw, enGB, DateTimeStyles.AllowWhiteSpaces, out var d2))
            return d2.ToString("yyyy-MM-dd");

        return ""; // keep empty on failure
    }

    static string BasicColour(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "";
        var s = raw.Trim().ToLowerInvariant();

        // fast keyword checks (UK spellings preferred)
        if (Regex.IsMatch(s, @"\bwhite\b")) return "White";
        if (Regex.IsMatch(s, @"\bblack\b|saphirschwarz|schwarz")) return "Black";
        if (Regex.IsMatch(s, @"\b(grey|gray|grau|graphite)\b")) return "Grey";
        if (Regex.IsMatch(s, @"\bsilver|silber\b")) return "Silver";
        if (Regex.IsMatch(s, @"\bblue|blau\b")) return "Blue";
        if (Regex.IsMatch(s, @"\bred|rosso|rot\b")) return "Red";
        if (Regex.IsMatch(s, @"\bgreen|grün|gruen\b")) return "Green";
        if (Regex.IsMatch(s, @"\byellow|gelb\b")) return "Yellow";
        if (Regex.IsMatch(s, @"\borange\b")) return "Orange";
        if (Regex.IsMatch(s, @"\bbrown|bronze|copper|chestnut|marron\b")) return "Brown";
        if (Regex.IsMatch(s, @"\bbeige|sand|champagne|ivory|cream\b")) return "Beige";
        if (Regex.IsMatch(s, @"\bgold|aurum\b")) return "Gold";
        if (Regex.IsMatch(s, @"\b(purple|violet|lilac|plum|amethyst)\b")) return "Purple";
        if (Regex.IsMatch(s, @"\b(pink|rose|rosé|magenta|fuchsia)\b")) return "Pink";

        // if we didn't find a match, just return the original text
        return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(s);
    }

    // ---------- Body style detection + normalization ----------
    static async Task<string> GetBodyStyleAsync(IPage page, string modelCode, string titleText)
    {
        // Try page data, JSON-LD, spec rows
        string raw = await page.EvaluateAsync<string>(@"
      () => {
        const clean = v => (typeof v === 'string' ? v.trim() : '');
        try {
          const adv = (window.UVL && (UVL.ADVERT || UVL.Advert || UVL.advert)) || {};
          const keys = ['body_style','bodyStyle','body_type','bodyType','bodystyle','vehicle_bodystyle','vehicleBodyStyle','category'];
          for (const k of keys) {
            const v = clean(adv[k]);
            if (v) return v;
          }
        } catch(e) {}

        try {
          for (const s of Array.from(document.querySelectorAll('script[type=""application/ld+json""]'))) {
            try {
              const obj = JSON.parse(s.textContent || '{}');
              const bt = clean(obj.bodyType || (obj.vehicleConfiguration && obj.vehicleConfiguration.bodyType));
              if (bt) return bt;
            } catch(e) {}
          }
        } catch(e) {}

        try {
          const rows = Array.from(document.querySelectorAll('.uvl-c-specification-details li.uvl-c-stat'));
          for (const li of rows) {
            const lab = clean(li.querySelector('.uvl-c-stat__title')?.textContent || '').toLowerCase();
            const val = clean(li.querySelector('.uvl-c-stat__value')?.textContent || '');
            if (/body\s*(type|style)/i.test(lab) && val) return val;
          }
        } catch(e) {}

        return '';
      }");

        // NEW: Infer from title (e.g., "... M Sport Saloon") if still empty
        if (string.IsNullOrWhiteSpace(raw))
        {
            var fromTitle = ExtractBodyStyleFromTitle(titleText);
            if (!string.IsNullOrWhiteSpace(fromTitle))
                raw = fromTitle;
        }

        // Fallback: model-code map
        if (string.IsNullOrWhiteSpace(raw))
            raw = MapModelCodeToBodyStyle(modelCode);

        return NormalizeBodyStyle(raw);
    }

    // NEW: robust extraction from title text / aria-label
    static string ExtractBodyStyleFromTitle(string title)
    {
        if (string.IsNullOrWhiteSpace(title)) return "";
        var t = title.ToLowerInvariant();

        var patterns = new (string pattern, string norm)[]
        {
            (@"gran\s*coup[ée]\b",                        "Gran Coupe"),
            (@"convertible|cabrio|cabriolet|roadster",    "Convertible"),
            (@"\btouring\b|\bestate\b",                   "Touring"),
            (@"\bsaloon\b|\bsedan\b",                     "Saloon"),
            (@"\bhatch(?:back)?\b|gran\s*turismo|\bgt\b", "Hatchback"),
            (@"\bcoup[ée]\b",                              "Coupe"),
            (@"active\s*tourer|\bmpv\b",                  "MPV"),
            (@"\bsuv\b|\bix\b|\bx[1-7]\b",                "SUV"),
        };

        foreach (var (pattern, norm) in patterns)
            if (Regex.IsMatch(t, pattern)) return norm;

        return "";
    }

    static string MapModelCodeToBodyStyle(string code)
    {
        code = (code ?? "").Trim().ToUpperInvariant();
        var m = Regex.Match(code, @"\b([FGIQU][0-9]{2})\b");
        var key = m.Success ? m.Groups[1].Value : code;

        var map = new Dictionary<string,string>(StringComparer.OrdinalIgnoreCase)
        {
            ["F20"]="Hatchback", ["F21"]="Hatchback", ["F40"]="Hatchback",
            ["F44"]="Gran Coupe", ["G42"]="Coupe",
            ["G20"]="Saloon", ["G21"]="Touring",
            ["G22"]="Coupe", ["G23"]="Convertible", ["G26"]="Gran Coupe",
            ["G30"]="Saloon", ["G31"]="Touring",
            ["G60"]="Saloon", ["G61"]="Touring",
            ["G70"]="Saloon",
            ["G14"]="Convertible", ["G15"]="Coupe", ["G16"]="Gran Coupe",
            ["F48"]="SUV", ["U11"]="SUV", ["F39"]="SUV", ["U10"]="SUV",
            ["G01"]="SUV", ["G02"]="SUV", ["G05"]="SUV", ["G06"]="SUV",
            ["G07"]="SUV", ["G08"]="SUV",
            ["I01"]="Hatchback", ["I20"]="SUV"
        };

        if (map.TryGetValue(key, out var s)) return s;
        return "";
    }

    static string NormalizeBodyStyle(string raw)
    {
        raw = (raw ?? "").Trim().ToLowerInvariant();

        if (Regex.IsMatch(raw, @"touring|estate")) return "Touring";
        if (Regex.IsMatch(raw, @"gran\s*coup[eé]")) return "Gran Coupe";
        if (Regex.IsMatch(raw, @"convertible|cabrio|cabriolet|roadster")) return "Convertible";
        if (Regex.IsMatch(raw, @"coupe|coup[eé]")) return "Coupe";
        if (Regex.IsMatch(raw, @"saloon|sedan")) return "Saloon";
        if (Regex.IsMatch(raw, @"hatch|fastback|gran\s*turismo|gt")) return "Hatchback";
        if (Regex.IsMatch(raw, @"mpv|tourer|active\s*tourer")) return "MPV";
        if (Regex.IsMatch(raw, @"suv|sport.*utility|x\d|ix")) return "SUV";

        return string.IsNullOrWhiteSpace(raw) ? "" :
               char.ToUpper(raw[0]) + raw.Substring(1);
    }
}
