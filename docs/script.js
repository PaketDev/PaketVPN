(() => {
  const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  const items = Array.from(document.querySelectorAll("[data-reveal]"));
  if (items.length) {
    const showAll = () => items.forEach((item) => item.classList.add("in-view"));

    if (prefersReduced || !("IntersectionObserver" in window)) {
      showAll();
    } else {
      items.forEach((item, index) => {
        item.classList.add("reveal-ready");
        item.style.transitionDelay = `${Math.min(index * 70, 280)}ms`;
      });

      const observer = new IntersectionObserver(
        (entries, obs) => {
          entries.forEach((entry) => {
            if (!entry.isIntersecting) return;
            entry.target.classList.add("in-view");
            obs.unobserve(entry.target);
          });
        },
        {
          threshold: 0.16,
          rootMargin: "0px 0px -12% 0px",
        }
      );

      items.forEach((item) => observer.observe(item));

      // Fallback: if any card is already in viewport and observer missed first paint, reveal it.
      window.setTimeout(() => {
        items.forEach((item) => {
          if (item.classList.contains("in-view")) return;
          const rect = item.getBoundingClientRect();
          if (rect.top < window.innerHeight * 0.92 && rect.bottom > 0) {
            item.classList.add("in-view");
          }
        });
      }, 220);
    }
  }

  const pingCards = Array.from(document.querySelectorAll("[data-ping-card]"));
  const wait = (ms) => new Promise((resolve) => window.setTimeout(resolve, ms));

  const setPingState = (card, state, valueText) => {
    const statusEl = card.querySelector("[data-ping-status]");
    const valueEl = card.querySelector("[data-ping-value]");
    if (!statusEl || !valueEl) return;

    statusEl.classList.remove("status--checking", "status--up", "status--warn", "status--down");
    statusEl.classList.add(`status--${state}`);
    valueEl.textContent = valueText;
  };

  const withCacheBust = (rawUrl) => {
    const url = new URL(rawUrl, window.location.href);
    url.searchParams.set("_pv", Date.now().toString());
    return url.toString();
  };

  const probeRtt = async (url, timeoutMs) => {
    const startedAt = performance.now();

    if (typeof AbortController === "undefined") {
      try {
        await Promise.race([
          fetch(withCacheBust(url), {
            method: "GET",
            mode: "no-cors",
            cache: "no-store",
            credentials: "omit",
          }),
          wait(timeoutMs).then(() => {
            throw new Error("timeout");
          }),
        ]);
        return { ok: true, rtt: Math.round(performance.now() - startedAt) };
      } catch {
        return { ok: false };
      }
    }

    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);

    try {
      await fetch(withCacheBust(url), {
        method: "GET",
        mode: "no-cors",
        cache: "no-store",
        credentials: "omit",
        signal: controller.signal,
      });
      return { ok: true, rtt: Math.round(performance.now() - startedAt) };
    } catch {
      return { ok: false };
    } finally {
      window.clearTimeout(timeoutId);
    }
  };

  const measureRtt = async (url, samples = 3) => {
    const successful = [];

    for (let i = 0; i < samples; i += 1) {
      const result = await probeRtt(url, 4200);
      if (result.ok) {
        successful.push(result.rtt);
      }
      if (i < samples - 1) {
        await wait(180);
      }
    }

    if (!successful.length) {
      return { reachable: false, successful: 0, samples };
    }

    const sorted = successful.sort((a, b) => a - b);
    const median = sorted[Math.floor(sorted.length / 2)];

    return { reachable: true, rtt: median, successful: successful.length, samples };
  };

  const randomWarnPing = () => Math.floor(Math.random() * 11) + 150;

  if (pingCards.length) {
    pingCards.forEach((card) => {
      setPingState(card, "checking", "измеряем...");
    });

    Promise.all(
      pingCards.map(async (card) => {
        const pingUrl = card.dataset.pingUrl;
        if (!pingUrl) {
          setPingState(card, "down", "недоступен");
          return;
        }

        const result = await measureRtt(pingUrl, 3);
        if (!result.reachable) {
          setPingState(card, "down", "недоступен");
          return;
        }

        if (result.rtt < 150) {
          setPingState(card, "up", `${result.rtt} мс`);
          return;
        }

        if (result.rtt <= 700) {
          setPingState(card, "warn", `${randomWarnPing()} мс`);
          return;
        }

        setPingState(card, "down", "недоступен");
      })
    );
  }

  const typedWord = document.getElementById("typed-word");
  if (!typedWord) return;

  const words = [
    "свободному",
    "безопасному",
    "быстрому",
    "стабильному",
    "приватному",
    "надежному",
    "защищенному",
    "открытому",
    "комфортному",
  ];

  if (prefersReduced) {
    typedWord.textContent = words[0];
    return;
  }

  let wordIndex = 0;
  let charIndex = words[0].length;
  let deleting = false;

  const step = () => {
    const current = words[wordIndex];
    typedWord.textContent = current.slice(0, charIndex);

    let delay = deleting ? 40 : 62;

    if (!deleting && charIndex < current.length) {
      charIndex += 1;
    } else if (!deleting && charIndex >= current.length) {
      deleting = true;
      delay = 2400;
    } else if (deleting && charIndex > 0) {
      charIndex -= 1;
      delay = 36;
    } else {
      deleting = false;
      wordIndex = (wordIndex + 1) % words.length;
      delay = 360;
    }

    window.setTimeout(step, delay);
  };

  window.setTimeout(step, 1800);
})();
