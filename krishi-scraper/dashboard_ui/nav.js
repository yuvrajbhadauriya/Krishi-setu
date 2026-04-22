/**
 * Krishi-Setu Shared Navigation
 * Include via: <script src="/ui/nav.js"></script>
 * Set active page: <html data-page="home|schemes|analytics|finder|eligibility|vault|master|crawler">
 */
(function () {
  const PATH_TO_PAGE = {
    '/': 'home',
    '/schemes': 'schemes',
    '/analytics': 'analytics',
    '/finder': 'finder',
    '/eligibility': 'eligibility',
    '/vault': 'vault',
    '/master': 'master',
    '/crawler': 'crawler',
  };
  const currentPath = (window.location.pathname || '/').replace(/\/+$/, '') || '/';
  const PAGE = document.documentElement.dataset.page || PATH_TO_PAGE[currentPath] || '';

  // ── Dark mode (shared) ───────────────────────────────────────────────────
  if (localStorage.getItem('ks-dark') === '1') {
    document.documentElement.classList.add('dark');
  }

  // ── Navigation config ────────────────────────────────────────────────────
  const NAV_LINKS = [
    { href: '/', page: 'home', label: 'Home', icon: 'home' },
    { href: '/schemes', page: 'schemes', label: 'All Schemes', icon: 'fact_check' },
    { href: '/analytics', page: 'analytics', label: 'Analytics', icon: 'insights' },
    { href: '/finder', page: 'finder', label: 'Scheme Finder', icon: 'search' },
    { href: '/eligibility', page: 'eligibility', label: 'Eligibility', icon: 'verified_user' },
    { href: '/vault', page: 'vault', label: 'My Documents', icon: 'folder' },
    { href: '/master', page: 'master', label: 'Admin', icon: 'admin_panel_settings' },
    { href: '/crawler', page: 'crawler', label: 'Crawler', icon: 'travel_explore' },
  ];

  const BOTTOM_NAV = [
    { href: '/', page: 'home', label: 'Home', icon: 'home' },
    { href: '/schemes', page: 'schemes', label: 'Schemes', icon: 'fact_check' },
    { href: '/analytics', page: 'analytics', label: 'Analytics', icon: 'insights' },
    { href: '/vault', page: 'vault', label: 'Vault', icon: 'folder' },
    { href: '/master', page: 'master', label: 'Admin', icon: 'admin_panel_settings' },
  ];

  window.KS_NAV_LINKS = NAV_LINKS;

  function isActive(p) { return p === PAGE; }

  // ── Avatar (initials-based) ──────────────────────────────────────────────
  function avatarHtml(name, role) {
    const initials = name.split(' ').map(w => w[0]).join('').slice(0, 2).toUpperCase();
    const colors = ['from-emerald-500 to-green-600', 'from-blue-500 to-cyan-600', 'from-violet-500 to-purple-600'];
    const c = colors[name.charCodeAt(0) % colors.length];
    return `<div class="size-9 rounded-full bg-gradient-to-br ${c} flex items-center justify-center text-white text-xs font-bold border-2 border-white dark:border-gray-700 shadow cursor-pointer" title="${name} · ${role}" onclick="window.ksNavToggleSidebar && window.ksNavToggleSidebar()">${initials}</div>`;
  }

  // ── Desktop top nav ──────────────────────────────────────────────────────
  function buildTopNav() {
    const visibleLinks = NAV_LINKS.slice(0, 6);
    const isDark = document.documentElement.classList.contains('dark');
    const darkIcon = isDark ? 'light_mode' : 'dark_mode';
    return `
<header id="ks-top-nav" class="sticky top-0 z-50 bg-white/95 dark:bg-[#102210]/95 backdrop-blur-md border-b border-gray-200 dark:border-gray-800 shadow-sm">
  <div class="max-w-[1440px] mx-auto px-4 h-16 flex items-center justify-between gap-4">
    <!-- Logo -->
    <a href="/" class="flex items-center gap-2 shrink-0">
      <div class="size-9 rounded-xl bg-gradient-to-br from-primary to-[#0dbb0d] flex items-center justify-center shadow-lg shadow-primary/30">
        <span class="material-symbols-outlined text-white text-xl" style="font-variation-settings:'FILL' 1">agriculture</span>
      </div>
      <div class="hidden sm:block">
        <p class="text-sm font-black tracking-tight text-[#111811] dark:text-white leading-none">Krishi-Setu</p>
        <p class="text-[10px] text-gray-500 dark:text-gray-400 leading-none">Agri DPI Platform</p>
      </div>
    </a>

    <!-- Desktop Nav Links -->
    <nav class="hidden lg:flex items-center gap-1">
      ${visibleLinks.map(l => `
        <a href="${l.href}" class="flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-all ${isActive(l.page)
          ? 'bg-primary/15 text-[#111811] dark:text-primary font-bold'
          : 'text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 hover:text-primary'}">
          <span class="material-symbols-outlined text-[18px]" style="font-variation-settings:'FILL' ${isActive(l.page) ? 1 : 0}">${l.icon}</span>
          ${l.label}
        </a>
      `).join('')}
    </nav>

    <!-- Right Controls -->
    <div class="flex items-center gap-2">
      <button id="ks-dark-btn" title="Toggle dark mode" class="size-9 flex items-center justify-center rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors">
        <span id="ks-dark-icon" class="material-symbols-outlined text-lg text-gray-600 dark:text-gray-300">${darkIcon}</span>
      </button>
      <a href="/analytics" title="Analytics" class="hidden md:flex size-9 items-center justify-center rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors ${isActive('analytics') ? 'text-primary' : 'text-gray-500'}">
        <span class="material-symbols-outlined text-lg">insights</span>
      </a>
      ${avatarHtml('Rajesh Kumar', 'Farmer')}
      <button id="ks-menu-btn" class="lg:hidden size-9 flex items-center justify-center rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors">
        <span class="material-symbols-outlined">menu</span>
      </button>
    </div>
  </div>
</header>
    `;
  }

  // ── Mobile bottom nav ────────────────────────────────────────────────────
  function buildBottomNav() {
    return `
<nav id="ks-bottom-nav" class="lg:hidden fixed bottom-0 left-0 right-0 z-40 bg-white/90 dark:bg-[#1a2e1a]/90 backdrop-blur-xl border-t border-gray-200 dark:border-gray-800 px-2 py-1 flex justify-around items-center shadow-[0_-4px_20px_rgba(0,0,0,0.08)]">
  ${BOTTOM_NAV.map((l, i) => i === 2
    ? `<a href="${l.href}" class="flex flex-col items-center justify-center -mt-7 size-14 rounded-full bg-gradient-to-br from-primary to-[#0dbb0d] shadow-xl shadow-primary/40 border-4 border-white dark:border-[#102210] ${isActive(l.page) ? 'ring-2 ring-primary ring-offset-1' : ''}">
        <span class="material-symbols-outlined text-white text-[24px]" style="font-variation-settings:'FILL' 1">${l.icon}</span>
      </a>`
    : `<a href="${l.href}" class="flex flex-col items-center gap-0.5 py-1 px-3 rounded-xl transition-colors ${isActive(l.page) ? 'text-primary' : 'text-gray-500 dark:text-gray-400 hover:text-primary'}">
        <span class="material-symbols-outlined text-[24px]" style="font-variation-settings:'FILL' ${isActive(l.page) ? 1 : 0}">${l.icon}</span>
        <span class="text-[9px] font-${isActive(l.page) ? 'bold' : 'medium'}">${l.label}</span>
      </a>`
  ).join('')}
</nav>
    `;
  }

  // ── Sidebar ──────────────────────────────────────────────────────────────
  function buildSidebar() {
    return `
<div id="ks-sidebar-overlay" class="fixed inset-0 z-[60] opacity-0 pointer-events-none transition-opacity duration-300">
  <div class="absolute inset-0 bg-black/40 backdrop-blur-[2px]" onclick="window.ksNavToggleSidebar()"></div>
  <aside id="ks-sidebar" class="absolute right-0 top-0 h-full w-[280px] bg-white dark:bg-[#1a2e1a] shadow-2xl flex flex-col translate-x-full transition-transform duration-300 overflow-y-auto">
    <!-- Header -->
    <div class="p-5 border-b border-gray-100 dark:border-gray-800 flex items-center gap-3">
      <div class="size-10 rounded-xl bg-gradient-to-br from-primary to-[#0dbb0d] flex items-center justify-center shadow">
        <span class="material-symbols-outlined text-white text-xl" style="font-variation-settings:'FILL' 1">agriculture</span>
      </div>
      <div class="flex-1">
        <p class="font-black text-base">Krishi-Setu</p>
        <p class="text-xs text-gray-500">Agri DPI Platform</p>
      </div>
      <button onclick="window.ksNavToggleSidebar()" class="size-8 flex items-center justify-center rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800">
        <span class="material-symbols-outlined text-sm">close</span>
      </button>
    </div>

    <!-- Profile card -->
    <div class="m-4 bg-gradient-to-br from-primary/10 to-emerald-50 dark:from-primary/10 dark:to-[#102210] rounded-2xl p-4 border border-primary/20">
      <div class="flex items-center gap-3 mb-3">
        <div class="size-12 rounded-full bg-gradient-to-br from-emerald-500 to-green-600 flex items-center justify-center text-white font-bold text-lg shadow-lg">RK</div>
        <div>
          <p class="font-bold text-sm">Rajesh Kumar</p>
          <p class="text-xs text-gray-500 dark:text-gray-400">Small Farmer • Maharashtra</p>
        </div>
      </div>
      <div class="grid grid-cols-3 gap-2 text-center">
        <div class="bg-white dark:bg-[#1a2e1a] rounded-xl p-2">
          <p class="text-xs font-bold text-primary" id="sidebar-schemes-count">—</p>
          <p class="text-[9px] text-gray-500">Schemes</p>
        </div>
        <div class="bg-white dark:bg-[#1a2e1a] rounded-xl p-2">
          <p class="text-xs font-bold text-blue-500">4</p>
          <p class="text-[9px] text-gray-500">Docs</p>
        </div>
        <div class="bg-white dark:bg-[#1a2e1a] rounded-xl p-2">
          <p class="text-xs font-bold text-amber-500">2</p>
          <p class="text-[9px] text-gray-500">Eligible</p>
        </div>
      </div>
    </div>

    <!-- Nav links -->
    <nav class="px-4 space-y-1 flex-1">
      ${NAV_LINKS.map(l => `
        <a href="${l.href}" class="flex items-center gap-3 px-3 py-2.5 rounded-xl transition-colors font-medium text-sm ${isActive(l.page)
          ? 'bg-primary/15 text-primary font-bold'
          : 'text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}">
          <span class="material-symbols-outlined text-[20px]" style="font-variation-settings:'FILL' ${isActive(l.page) ? 1 : 0}">${l.icon}</span>
          ${l.label}
          ${isActive(l.page) ? '<span class="ml-auto size-1.5 rounded-full bg-primary"></span>' : ''}
        </a>
      `).join('')}
    </nav>

    <!-- Footer -->
    <div class="p-4 border-t border-gray-100 dark:border-gray-800 text-center text-xs text-gray-400">
      Krishi-Setu v2.0 · Government Verified Platform
    </div>
  </aside>
</div>
    `;
  }

  // ── Inject into DOM ──────────────────────────────────────────────────────
  function inject() {
    if (document.getElementById('ks-top-nav')) return;

    document.body.classList.add('ks-shared-nav');
    if (!document.getElementById('ks-nav-shared-style')) {
      const style = document.createElement('style');
      style.id = 'ks-nav-shared-style';
      style.textContent = `
        body.ks-shared-nav .sticky.top-0:not(#ks-top-nav) { top: 4rem; }
        body.ks-shared-nav .ks-legacy-bottom-nav { display: none !important; }
        @media (max-width: 1023px) {
          body.ks-shared-nav { padding-bottom: 4.75rem; }
        }
      `;
      document.head.appendChild(style);
    }

    // Top nav — insert before first child of body
    const topNavEl = document.createElement('div');
    topNavEl.innerHTML = buildTopNav();
    document.body.insertBefore(topNavEl.firstElementChild, document.body.firstChild);

    // Bottom nav
    const bottomEl = document.createElement('div');
    bottomEl.innerHTML = buildBottomNav();
    document.body.appendChild(bottomEl.firstElementChild);

    // Sidebar
    const sidebarEl = document.createElement('div');
    sidebarEl.innerHTML = buildSidebar();
    document.body.appendChild(sidebarEl.firstElementChild);

    // Dark mode button
    const darkBtn = document.getElementById('ks-dark-btn');
    if (darkBtn) {
      darkBtn.addEventListener('click', () => {
        const isDark = document.documentElement.classList.toggle('dark');
        localStorage.setItem('ks-dark', isDark ? '1' : '0');
        const icon = document.getElementById('ks-dark-icon');
        if (icon) icon.textContent = isDark ? 'light_mode' : 'dark_mode';
      });
    }

    // Menu button
    const menuBtn = document.getElementById('ks-menu-btn');
    if (menuBtn) menuBtn.addEventListener('click', () => window.ksNavToggleSidebar());

    // Load schemes count for sidebar
    fetch('/api/master/overview').then(r => r.json()).then(d => {
      const el = document.getElementById('sidebar-schemes-count');
      if (el) el.textContent = d.totals?.master_schemes || d.totals?.master || '—';
    }).catch(() => {});
  }

  // ── Sidebar toggle ────────────────────────────────────────────────────────
  window.ksNavToggleSidebar = function () {
    const overlay = document.getElementById('ks-sidebar-overlay');
    const sidebar = document.getElementById('ks-sidebar');
    if (!overlay || !sidebar) return;
    const isOpen = !overlay.classList.contains('pointer-events-none');
    if (isOpen) {
      overlay.classList.add('opacity-0', 'pointer-events-none');
      sidebar.classList.add('translate-x-full');
      document.body.style.overflow = '';
    } else {
      overlay.classList.remove('opacity-0', 'pointer-events-none');
      sidebar.classList.remove('translate-x-full');
      document.body.style.overflow = 'hidden';
    }
  };

  // ── Toast utility (global) ───────────────────────────────────────────────
  window.ksToast = function (message, icon = 'info', type = 'default') {
    const existing = document.getElementById('ks-global-toast');
    if (existing) existing.remove();
    const colors = type === 'error' ? 'bg-red-600 text-white' : type === 'success' ? 'bg-emerald-600 text-white' : 'bg-[#111811] dark:bg-white text-white dark:text-[#111811]';
    // Icon color: use primary on success/error backgrounds, white on dark backgrounds
    const iconColor = (type === 'error' || type === 'success') ? 'text-white' : 'text-primary dark:text-[#111811]';
    const t = document.createElement('div');
    t.id = 'ks-global-toast';
    t.className = `fixed top-20 right-4 z-[100] ${colors} px-4 py-3 rounded-2xl shadow-2xl flex items-center gap-2 text-sm font-semibold max-w-xs transition-all duration-300`;
    t.innerHTML = `<span class="material-symbols-outlined ${iconColor} text-sm" style="font-variation-settings:'FILL' 1">${icon}</span>${message}`;
    document.body.appendChild(t);
    setTimeout(() => { t.style.opacity = '0'; t.style.transform = 'translateY(-8px)'; }, 2200);
    setTimeout(() => t.remove(), 2700);
  };

  // Run when DOM is ready
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', inject);
  } else {
    inject();
  }
})();
