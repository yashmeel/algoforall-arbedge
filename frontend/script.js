// AlgoForAll ArbEdge Frontend Script

// Mobile Nav
const hamburger = document.getElementById('hamburger');
const mobileNav = document.getElementById('mobileNav');
const mobileNavClose = document.getElementById('mobileNavClose');
const mobileNavOverlay = document.getElementById('mobileNavOverlay');

function openNav() {
  mobileNav.classList.add('open');
  mobileNavOverlay.classList.add('open');
  document.body.style.overflow = 'hidden';
}
function closeNav() {
  mobileNav.classList.remove('open');
  mobileNavOverlay.classList.remove('open');
  document.body.style.overflow = '';
}

hamburger?.addEventListener('click', openNav);
mobileNavClose?.addEventListener('click', closeNav);
mobileNavOverlay?.addEventListener('click', closeNav);

// Sticky header shadow on scroll
const header = document.getElementById('header');
window.addEventListener('scroll', () => {
  header.style.boxShadow = window.scrollY > 10 ? '0 2px 20px rgba(0,0,0,0.4)' : 'none';
});

// Animate numbers in stats bar on scroll
const statNums = document.querySelectorAll('.stat-num');
const observer = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('animated');
      observer.unobserve(entry.target);
    }
  });
}, { threshold: 0.5 });
statNums.forEach(el => observer.observe(el));

// Mockup tab switching
document.querySelectorAll('.mtab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.mtab').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
  });
});
