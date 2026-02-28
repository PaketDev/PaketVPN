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
