const nav = document.querySelector(".nav");
const faqItems = Array.from(document.querySelectorAll(".faq details"));
const revealItems = document.querySelectorAll(".reveal");
const anchors = Array.from(document.querySelectorAll('a[href^="#"]'));

const handleScroll = () => {
  if (!nav) return;
  nav.classList.toggle("nav--floating", window.scrollY > 12);
};

const handleFaqToggle = (current) => {
  faqItems.forEach((item) => {
    if (item !== current) {
      item.removeAttribute("open");
    }
  });
};

faqItems.forEach((item) => {
  item.addEventListener("toggle", () => {
    if (item.open) {
      handleFaqToggle(item);
    }
  });
});

handleScroll();
window.addEventListener("scroll", handleScroll);

if (revealItems.length) {
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("reveal--visible");
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.2 }
  );

  revealItems.forEach((item) => observer.observe(item));
}

anchors.forEach((link) => {
  link.addEventListener("click", (e) => {
    const href = link.getAttribute("href");
    if (!href || !href.startsWith("#")) return;
    const target = document.querySelector(href);
    if (!target) return;
    e.preventDefault();
    target.scrollIntoView({ behavior: "smooth", block: "start" });
  });
});
