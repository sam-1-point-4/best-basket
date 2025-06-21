import puppeteerExtra from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import * as fs from "fs";
import * as readline from "readline";

puppeteerExtra.use(StealthPlugin());

// Struktur für das Ergebnis pro Produkt-Link
interface ProductResult {
  url: string; // Produkt-URL
  name: string; // Produktname
  price: string; // Preis (z.B. "2,99 €")
  unitPrice: string; // Preis pro Stück oder pro Kilo
}

/**
 * Liest alle Links (jede zweite Zeile) aus der wolt-liste.txt ein.
 * Erwartet: abwechselnd Suchbegriff und Link, Link beginnt mit "http"
 */
async function readLinksFromFile(
  filePath: string
): Promise<{ url: string; query: string }[]> {
  return new Promise((resolve, reject) => {
    const links: { url: string; query: string }[] = [];
    let lastQuery = "";
    const rl = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity,
    });
    rl.on("line", (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      if (trimmed.startsWith("http")) {
        links.push({ url: trimmed, query: lastQuery });
      } else {
        lastQuery = trimmed.replace(/:$/, "");
      }
    });
    rl.on("close", () => resolve(links));
    rl.on("error", reject);
  });
}

/**
 * Scrapt für jeden Produktlink die wichtigsten Infos aus dem Produkt-Modal.
 */
export async function scrapeWoltLinks(): Promise<ProductResult[]> {
  const browser = await puppeteerExtra.launch({
    headless: false,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  try {
    const page = await browser.newPage();
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, wie Gecko) Chrome/120.0.0.0 Safari/537.36"
    );
    await page.setViewport({ width: 1920, height: 1080 });

    // 1. Links aus Datei einlesen
    const links = await readLinksFromFile("wolt-liste.txt");
    const results: ProductResult[] = [];

    for (const { url } of links) {
      console.log(`Rufe Produktseite auf: ${url}`);
      await page.goto(url, { waitUntil: "networkidle2" });

      // 2. Cookie-Banner akzeptieren (nur beim ersten Mal nötig)
      try {
        await page.waitForSelector("button", { timeout: 10000 });
        const cookieButtons = await page.$$("button");
        let clicked = false;
        for (const btn of cookieButtons) {
          const text = await page.evaluate((el) => el.textContent, btn);
          if (text && text.trim().toLowerCase().includes("erlauben")) {
            await btn.click();
            clicked = true;
            console.log("Cookie-Banner akzeptiert.");
            await new Promise((resolve) => setTimeout(resolve, 1000));
            break;
          }
        }
        if (!clicked) {
          console.log('Kein "Erlauben"-Button gefunden.');
        }
      } catch (e) {
        console.log("Kein Cookie-Banner gefunden oder bereits akzeptiert.");
      }

      // 3. Warte auf das Produkt-Modal
      try {
        await page.waitForSelector('aside[data-test-id="product-modal"]', {
          timeout: 10000,
        });
        // Produktdaten extrahieren
        const produkt = await page.evaluate(() => {
          const modal = document.querySelector(
            'aside[data-test-id="product-modal"]'
          );
          if (!modal) return null;
          const nameEl = modal.querySelector("h2.h1m8nnah");
          const priceEl = modal.querySelector(
            'span[data-test-id="product-modal.price"]'
          );
          const unitPriceEl = modal.querySelector(
            'span[data-test-id="product-modal.unit-price"]'
          );
          const name = nameEl ? nameEl.textContent?.trim() || "" : "";
          const price = priceEl ? priceEl.textContent?.trim() || "" : "";
          const unitPrice = unitPriceEl
            ? unitPriceEl.textContent?.trim() || ""
            : "";
          return { name, price, unitPrice };
        });
        if (produkt && produkt.name && produkt.price) {
          results.push({ url, ...produkt });
          console.log(
            `Erfasst: ${produkt.name} - ${produkt.price} (${produkt.unitPrice})`
          );
        } else {
          console.log(`Keine Produktdaten gefunden für ${url}`);
        }
      } catch (e) {
        console.log(`Fehler beim Extrahieren für ${url}`);
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    fs.writeFileSync("wolt-products.json", JSON.stringify(results, null, 2));
    console.log("Alle Produktdaten wurden in wolt-products.json gespeichert");
    return results;
  } catch (error) {
    console.error("Fehler beim Scraping:", error);
    return [];
  } finally {
    await browser.close();
  }
}

// Starte den Scraper direkt
scrapeWoltLinks().catch((error) => {
  console.error("Fehler beim Ausführen des Wolt-Link-Scrapers:", error);
});

//npm run scrape:wolt
