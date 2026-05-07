const state = {
  dashboard: null,
  wallets: [],
  markets: [],
  selectedWalletAddress: null,
  walletSort: { key: "accountValue", direction: "desc" },
  discoverySort: { key: "discoveryScore", direction: "desc" },
  live: {
    walletSocket: null,
    discoverySocket: null,
    walletRefreshTimer: null,
    discoveredAddresses: new Set(),
    discoveredTrades: 0,
    discoveryRunning: false,
  },
};

const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 0,
});

const compactFormatter = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 2,
});

const percentFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 1,
});

function formatMoney(value) {
  return currencyFormatter.format(Number(value || 0));
}

function formatCompactMoney(value) {
  const numeric = Number(value || 0);
  return `${numeric < 0 ? "-" : ""}$${compactFormatter.format(Math.abs(numeric))}`;
}

function formatDate(value) {
  if (!value) return "n/a";
  const date = typeof value === "number" ? new Date(value) : new Date(String(value));
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function shortAddress(address) {
  if (!address) return "";
  return `${address.slice(0, 6)}...${address.slice(-4)}`;
}

function setMessage(message, tone = "neutral") {
  const node = document.getElementById("message-box");
  node.textContent = message;
  node.dataset.tone = tone;
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Request failed");
  return data;
}

function sortItems(items, { key, direction }) {
  const factor = direction === "asc" ? 1 : -1;
  return [...items].sort((left, right) => {
    const leftValue = key.split(".").reduce((acc, part) => acc?.[part], left);
    const rightValue = key.split(".").reduce((acc, part) => acc?.[part], right);

    if (typeof leftValue === "string" || typeof rightValue === "string") {
      return String(leftValue || "").localeCompare(String(rightValue || "")) * factor;
    }
    return ((Number(leftValue) || 0) - (Number(rightValue) || 0)) * factor;
  });
}

function toggleSort(current, key) {
  if (current.key === key) {
    current.direction = current.direction === "asc" ? "desc" : "asc";
  } else {
    current.key = key;
    current.direction = "desc";
  }
}

function renderStats() {
  const root = document.getElementById("stats-grid");
  if (!state.dashboard) {
    root.innerHTML = "";
    return;
  }
  const totals = state.dashboard.totals;
  const cards = [
    ["Wallets", totals.walletsTracked],
    ["Account Value", formatMoney(totals.combinedAccountValue)],
    ["Open Notional", formatMoney(totals.combinedNotional)],
    ["Realized PnL", formatMoney(totals.combinedRealizedPnl)],
    ["Unrealized PnL", formatMoney(totals.combinedUnrealizedPnl)],
    ["Money Printers", totals.moneyPrinterWallets],
    ["Signals", state.dashboard.sentiment?.signalCount || 0],
  ];
  root.innerHTML = cards
    .map(
      ([label, value]) => `
        <article class="stat-card">
          <span>${label}</span>
          <strong>${value}</strong>
        </article>
      `
    )
    .join("");
}

function renderSignals() {
  const root = document.getElementById("signals-list");
  const signals = state.dashboard?.sentiment?.signals || [];
  if (!signals.length) {
    root.className = "signal-grid empty-state";
    root.textContent = "No high-conviction signals yet. Add wallets or refresh when conviction improves.";
    return;
  }
  root.className = "signal-grid";
  root.innerHTML = signals
    .slice(0, 6)
    .map(
      (signal) => `
        <article class="signal-card ${signal.action === "sell" ? "short" : "long"}">
          <div>
            <span>${signal.strength} conviction</span>
            <strong>${String(signal.action || "watch").toUpperCase()} ${signal.coin}</strong>
          </div>
          <p>${signal.side} from ${signal.walletCount} wallets with ${formatCompactMoney(signal.totalValue)} notional.</p>
          <div class="signal-meter">
            <span style="width: ${Math.min(Number(signal.convictionScore || 0), 100)}%"></span>
          </div>
          <small>${percentFormatter.format(signal.convictionScore || 0)}/100 conviction</small>
        </article>
      `
    )
    .join("");
}

function renderSegments() {
  const root = document.getElementById("segments-list");
  const segments = state.dashboard?.segments || [];
  if (!segments.length) {
    root.className = "stack-list empty-state";
    root.textContent = "Refresh to generate segments.";
    return;
  }
  root.className = "stack-list";
  root.innerHTML = segments
    .map(
      (segment) => `
        <article class="list-card">
          <div>
            <strong>${segment.label}</strong>
            <p>${segment.count} wallet${segment.count === 1 ? "" : "s"}</p>
          </div>
          <div class="list-card-metrics">
            <span>${formatCompactMoney(segment.combinedAccountValue)}</span>
            <span>${formatCompactMoney(segment.netExposure)} net</span>
          </div>
        </article>
      `
    )
    .join("");
}

function renderSavedWallets() {
  const root = document.getElementById("saved-wallets");
  const wallets = state.dashboard?.savedWallets || state.wallets;
  if (!wallets.length) {
    root.className = "stack-list empty-state";
    root.textContent = "No wallets saved yet.";
    return;
  }
  root.className = "stack-list";
  root.innerHTML = wallets
    .map(
      (wallet) => `
        <article class="list-card">
          <div>
            <strong>${wallet.alias || "Unnamed wallet"}</strong>
            <p>${wallet.address}</p>
          </div>
          <button class="danger-btn" data-address="${wallet.address}">Remove</button>
        </article>
      `
    )
    .join("");

  root.querySelectorAll("[data-address]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await request(`/api/wallets/${button.dataset.address}`, { method: "DELETE" });
        await loadWallets();
        await refreshDashboard(false);
        reconnectWalletSocket();
        setMessage("Wallet removed.", "success");
      } catch (error) {
        setMessage(error.message, "error");
      }
    });
  });
}

function getFilteredWallets() {
  const query = document.getElementById("wallet-search").value.trim().toLowerCase();
  const wallets = sortItems(state.dashboard?.wallets || [], state.walletSort);
  if (!query) return wallets;
  return wallets.filter((wallet) => {
    return [
      wallet.alias,
      wallet.address,
      wallet.notes,
      wallet.cohorts.walletSize,
      wallet.cohorts.profitability,
      wallet.recentWinRateRank?.label,
    ]
      .join(" ")
      .toLowerCase()
      .includes(query);
  });
}

function renderWalletTable() {
  const root = document.getElementById("wallet-table-wrap");
  const wallets = getFilteredWallets();
  if (!wallets.length) {
    root.className = "table-wrap empty-state";
    root.textContent = "Add wallets to populate the tracker table.";
    return;
  }

  if (!state.selectedWalletAddress || !wallets.find((wallet) => wallet.address === state.selectedWalletAddress)) {
    state.selectedWalletAddress = wallets[0].address;
  }

  const columns = [
    ["Alias", "alias"],
    ["Account Value", "accountValue"],
    ["Realized PnL", "realizedPnl"],
    ["uPnL", "unrealizedPnl"],
    ["Exposure", "exposure.net"],
    ["Positions", "positionCount"],
    ["Orders", "openOrderCount"],
    ["7D Hit Rate", "hitRate"],
    ["7D Rank", "recentWinRateRank.score"],
  ];

  root.className = "table-wrap";
  root.innerHTML = `
    <table class="data-table">
      <thead>
        <tr>
          ${columns
            .map(
              ([label, key]) => `
                <th>
                  <button class="sort-button" data-key="${key}">
                    ${label}
                    ${state.walletSort.key === key ? `<span>${state.walletSort.direction === "asc" ? "↑" : "↓"}</span>` : ""}
                  </button>
                </th>
              `
            )
            .join("")}
          <th>Cohorts</th>
          <th>Address</th>
        </tr>
      </thead>
      <tbody>
        ${wallets
          .map(
            (wallet) => `
              <tr class="${wallet.address === state.selectedWalletAddress ? "selected-row" : ""}" data-wallet-row="${wallet.address}">
                <td>${wallet.alias || "Unnamed wallet"}</td>
                <td>${formatMoney(wallet.accountValue)}</td>
                <td class="${wallet.realizedPnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.realizedPnl)}</td>
                <td class="${wallet.unrealizedPnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.unrealizedPnl)}</td>
                <td>${formatMoney(wallet.exposure.net)}</td>
                <td>${wallet.positionCount}</td>
                <td>${wallet.openOrderCount}</td>
                <td>${percentFormatter.format(wallet.hitRate)}%</td>
                <td>${wallet.recentWinRateRank?.label || "Unranked"} (${percentFormatter.format(wallet.recentWinRateRank?.score || 0)})</td>
                <td>${wallet.cohorts.walletSize} / ${wallet.cohorts.profitability}</td>
                <td>${shortAddress(wallet.address)}</td>
              </tr>
            `
          )
          .join("")}
      </tbody>
    </table>
  `;

  root.querySelectorAll(".sort-button").forEach((button) => {
    button.addEventListener("click", () => {
      toggleSort(state.walletSort, button.dataset.key);
      renderWalletTable();
    });
  });

  root.querySelectorAll("[data-wallet-row]").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedWalletAddress = row.dataset.walletRow;
      renderWalletTable();
      renderWalletDetails();
    });
  });
}

function renderWalletDetails() {
  const root = document.getElementById("wallet-details");
  const wallet = (state.dashboard?.wallets || []).find((entry) => entry.address === state.selectedWalletAddress);
  if (!wallet) {
    root.className = "wallet-detail empty-state";
    root.textContent = "Select a wallet row to inspect positions and fills.";
    return;
  }

  const positionRows = wallet.positions.length
    ? wallet.positions
        .map(
          (position) => `
            <tr>
              <td>${position.coin}</td>
              <td>${position.side}</td>
              <td>${Number(position.size).toFixed(3)}</td>
              <td>${formatMoney(position.positionValue)}</td>
              <td class="${position.unrealizedPnl >= 0 ? "positive" : "negative"}">${formatMoney(position.unrealizedPnl)}</td>
            </tr>
          `
        )
        .join("")
    : `<tr><td colspan="5">No open positions.</td></tr>`;

  const fills = wallet.recentFills.length
    ? wallet.recentFills
        .slice(0, 8)
        .map(
          (fill) => `
            <li>
              <span>${fill.coin} ${fill.direction}</span>
              <span>${formatDate(fill.time)}</span>
              <strong class="${fill.closedPnl >= 0 ? "positive" : "negative"}">${formatMoney(fill.closedPnl)}</strong>
            </li>
          `
        )
        .join("")
    : "<li><span>No recent fills.</span><span>n/a</span><strong>n/a</strong></li>";

  root.className = "wallet-detail";
  root.innerHTML = `
    <div class="wallet-summary">
      <div>
        <p class="wallet-title">${wallet.alias || "Unnamed wallet"}</p>
        <h3>${wallet.address}</h3>
      </div>
      <span class="pill">${wallet.role}</span>
    </div>

    <div class="wallet-metrics">
      <div><span>Wallet Size</span><strong>${wallet.cohorts.walletSize}</strong></div>
      <div><span>Profitability</span><strong>${wallet.cohorts.profitability}</strong></div>
      <div><span>7D Rank</span><strong>${wallet.recentWinRateRank?.label || "Unranked"} (${percentFormatter.format(wallet.recentWinRateRank?.score || 0)})</strong></div>
      <div><span>7D Hit Rate</span><strong>${percentFormatter.format(wallet.hitRate)}% / ${wallet.recentClosedTrades || 0} closes</strong></div>
      <div><span>7D PnL</span><strong class="${wallet.recentRealizedPnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.recentRealizedPnl)}</strong></div>
      <div><span>Day PnL</span><strong class="${wallet.performance.day.pnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.performance.day.pnl)}</strong></div>
      <div><span>Week PnL</span><strong class="${wallet.performance.week.pnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.performance.week.pnl)}</strong></div>
      <div><span>Month PnL</span><strong class="${wallet.performance.month.pnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.performance.month.pnl)}</strong></div>
      <div><span>All-time PnL</span><strong class="${wallet.realizedPnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.realizedPnl)}</strong></div>
    </div>

    <div class="wallet-detail-grid">
      <section>
        <div class="subsection-head">
          <strong>Open positions</strong>
          <span>${wallet.positionCount} live</span>
        </div>
        <table class="data-table">
          <thead>
            <tr><th>Coin</th><th>Side</th><th>Size</th><th>Value</th><th>uPnL</th></tr>
          </thead>
          <tbody>${positionRows}</tbody>
        </table>
      </section>
      <section>
        <div class="subsection-head">
          <strong>Recent fills</strong>
          <span>${wallet.openOrderCount} open orders</span>
        </div>
        <ul class="fills-list">${fills}</ul>
      </section>
    </div>
  `;
}

function renderDiscoveryResults(candidates = []) {
  const root = document.getElementById("discovery-results");
  if (!candidates.length) {
    root.className = "table-wrap empty-state";
    root.textContent = "Start discovery, then score candidates to surface fresh wallets.";
    return;
  }

  const sorted = sortItems(candidates, state.discoverySort);
  const columns = [
    ["Score", "discoveryScore"],
    ["Account", "accountValue"],
    ["Realized", "realizedPnl"],
    ["Notional", "totalNotional"],
  ];

  root.className = "table-wrap";
  root.innerHTML = `
    <table class="data-table">
      <thead>
        <tr>
          ${columns
            .map(
              ([label, key]) => `
                <th>
                  <button class="sort-button discovery-sort" data-key="${key}">
                    ${label}
                    ${state.discoverySort.key === key ? `<span>${state.discoverySort.direction === "asc" ? "↑" : "↓"}</span>` : ""}
                  </button>
                </th>
              `
            )
            .join("")}
          <th>Cohorts</th>
          <th>Address</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        ${sorted
          .map(
            (wallet) => `
              <tr>
                <td>${formatCompactMoney(wallet.discoveryScore)}</td>
                <td>${formatMoney(wallet.accountValue)}</td>
                <td class="${wallet.realizedPnl >= 0 ? "positive" : "negative"}">${formatMoney(wallet.realizedPnl)}</td>
                <td>${formatMoney(wallet.totalNotional)}</td>
                <td>${wallet.cohorts.walletSize} / ${wallet.cohorts.profitability}</td>
                <td>${shortAddress(wallet.address)}</td>
                <td><button class="secondary-btn add-discovered" data-address="${wallet.address}">Track</button></td>
              </tr>
            `
          )
          .join("")}
      </tbody>
    </table>
  `;

  root.querySelectorAll(".discovery-sort").forEach((button) => {
    button.addEventListener("click", () => {
      toggleSort(state.discoverySort, button.dataset.key);
      renderDiscoveryResults(candidates);
    });
  });

  root.querySelectorAll(".add-discovered").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await request("/api/wallets", {
          method: "POST",
          body: JSON.stringify({ address: button.dataset.address, alias: "Discovered wallet", notes: "Added from discovery engine" }),
        });
        await loadWallets();
        await refreshDashboard(false);
        reconnectWalletSocket();
        setMessage("Discovered wallet added to watchlist.", "success");
      } catch (error) {
        setMessage(error.message, "error");
      }
    });
  });
}

function renderMarketsPicker() {
  const root = document.getElementById("markets-picker");
  const defaults = new Set(["BTC", "ETH", "SOL"]);
  root.innerHTML = state.markets
    .slice(0, 18)
    .map(
      (market) => `
        <label class="market-chip">
          <input type="checkbox" value="${market}" ${defaults.has(market) ? "checked" : ""} />
          <span>${market}</span>
        </label>
      `
    )
    .join("");
}

function updateDiscoveryCounters() {
  document.getElementById("discovered-count").textContent = state.live.discoveredAddresses.size;
  document.getElementById("trade-count").textContent = state.live.discoveredTrades;
}

async function loadHealth() {
  try {
    const data = await request("/api/health");
    document.getElementById("health-status").textContent = data.ok ? "Connected" : "Offline";
  } catch {
    document.getElementById("health-status").textContent = "Offline";
  }
}

async function loadWallets() {
  const data = await request("/api/wallets");
  state.wallets = data.wallets;
  renderSavedWallets();
}

async function loadMarkets() {
  const data = await request("/api/markets");
  state.markets = data.markets;
  renderMarketsPicker();
}

async function refreshDashboard(showMessage = true) {
  const button = document.getElementById("refresh-button");
  button.disabled = true;
  button.textContent = "Refreshing...";
  if (showMessage) setMessage("Refreshing dashboard from Hyperliquid...", "neutral");
  try {
    state.dashboard = await request("/api/dashboard");
    document.getElementById("last-updated").textContent = formatDate(state.dashboard.generatedAt);
    renderStats();
    renderSignals();
    renderSegments();
    renderSavedWallets();
    renderWalletTable();
    renderWalletDetails();
    if (showMessage) setMessage("Dashboard refreshed.", "success");
  } catch (error) {
    setMessage(error.message, "error");
  } finally {
    button.disabled = false;
    button.textContent = "Refresh";
  }
}

function getSelectedDiscoveryMarkets() {
  return [...document.querySelectorAll("#markets-picker input:checked")].map((input) => input.value);
}

function debounceWalletRefresh() {
  if (state.live.walletRefreshTimer) clearTimeout(state.live.walletRefreshTimer);
  state.live.walletRefreshTimer = setTimeout(() => {
    refreshDashboard(false);
  }, 1200);
}

function closeSocket(socket) {
  if (socket) {
    socket.onclose = null;
    socket.close();
  }
}

function reconnectWalletSocket() {
  closeSocket(state.live.walletSocket);
  const trackedWallets = state.wallets;
  if (!trackedWallets.length) {
    document.getElementById("wallet-ws-status").textContent = "Idle";
    return;
  }

  const socket = new WebSocket("wss://api.hyperliquid.xyz/ws");
  state.live.walletSocket = socket;
  document.getElementById("wallet-ws-status").textContent = "Connecting";

  socket.onopen = () => {
    document.getElementById("wallet-ws-status").textContent = "Live";
    trackedWallets.forEach((wallet) => {
      [
        { type: "clearinghouseState", user: wallet.address },
        { type: "userFills", user: wallet.address, aggregateByTime: true },
        { type: "orderUpdates", user: wallet.address },
      ].forEach((subscription) => {
        socket.send(JSON.stringify({ method: "subscribe", subscription }));
      });
    });
  };

  socket.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    if (["clearinghouseState", "userFills", "orderUpdates", "userEvents"].includes(payload.channel)) {
      debounceWalletRefresh();
    }
  };

  socket.onclose = () => {
    document.getElementById("wallet-ws-status").textContent = "Reconnecting";
    if (state.wallets.length) {
      setTimeout(reconnectWalletSocket, 2500);
    }
  };

  socket.onerror = () => {
    document.getElementById("wallet-ws-status").textContent = "Error";
  };
}

function startDiscoverySocket() {
  closeSocket(state.live.discoverySocket);
  state.live.discoveryRunning = true;
  const markets = getSelectedDiscoveryMarkets();
  if (!markets.length) {
    setMessage("Choose at least one market for discovery.", "error");
    state.live.discoveryRunning = false;
    return;
  }

  const socket = new WebSocket("wss://api.hyperliquid.xyz/ws");
  state.live.discoverySocket = socket;
  document.getElementById("discovery-ws-status").textContent = "Connecting";
  document.getElementById("discover-toggle").textContent = "Stop Discovery";

  socket.onopen = () => {
    document.getElementById("discovery-ws-status").textContent = `Live (${markets.length})`;
    markets.forEach((market) => {
      socket.send(JSON.stringify({ method: "subscribe", subscription: { type: "trades", coin: market } }));
    });
  };

  socket.onmessage = (event) => {
    const payload = JSON.parse(event.data);
    if (payload.channel !== "trades" || !Array.isArray(payload.data)) return;
    payload.data.forEach((trade) => {
      state.live.discoveredTrades += 1;
      (trade.users || []).forEach((address) => {
        if (!state.wallets.find((wallet) => wallet.address.toLowerCase() === String(address).toLowerCase())) {
          state.live.discoveredAddresses.add(address);
        }
      });
    });
    updateDiscoveryCounters();
  };

  socket.onclose = () => {
    document.getElementById("discovery-ws-status").textContent = state.live.discoveryRunning ? "Reconnecting" : "Idle";
    if (state.live.discoveryRunning) {
      setTimeout(startDiscoverySocket, 2500);
    }
  };

  socket.onerror = () => {
    document.getElementById("discovery-ws-status").textContent = "Error";
  };
}

function stopDiscoverySocket() {
  state.live.discoveryRunning = false;
  closeSocket(state.live.discoverySocket);
  state.live.discoverySocket = null;
  document.getElementById("discovery-ws-status").textContent = "Idle";
  document.getElementById("discover-toggle").textContent = "Start Discovery";
}

async function scoreDiscoveredCandidates() {
  const addresses = [...state.live.discoveredAddresses];
  if (!addresses.length) {
    setMessage("No discovered addresses yet. Start the discovery feed first.", "error");
    return;
  }

  setMessage(`Scoring ${addresses.length} discovered addresses...`, "neutral");
  try {
    const result = await request("/api/discovery/scan", {
      method: "POST",
      body: JSON.stringify({
        addresses,
        limit: 20,
        minAccountValue: Number(document.getElementById("min-account-value").value || 0),
        minRealizedPnl: Number(document.getElementById("min-realized-pnl").value || 0),
      }),
    });
    renderDiscoveryResults(result.candidates || []);
    setMessage(`Scanned ${result.scanned} addresses and ranked ${result.candidates.length} candidates.`, "success");
  } catch (error) {
    setMessage(error.message, "error");
  }
}

function installFormHandlers() {
  document.getElementById("wallet-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await request("/api/wallets", {
        method: "POST",
        body: JSON.stringify({
          alias: document.getElementById("alias").value.trim(),
          address: document.getElementById("address").value.trim(),
          notes: document.getElementById("notes").value.trim(),
        }),
      });
      event.target.reset();
      await loadWallets();
      await refreshDashboard(false);
      reconnectWalletSocket();
      setMessage("Wallet saved.", "success");
    } catch (error) {
      setMessage(error.message, "error");
    }
  });

  document.getElementById("import-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const text = document.getElementById("import-text").value.trim();
    if (!text) {
      setMessage("Paste at least one wallet line to import.", "error");
      return;
    }
    try {
      const result = await request("/api/wallets/import", {
        method: "POST",
        body: JSON.stringify({ text }),
      });
      document.getElementById("import-text").value = "";
      await loadWallets();
      await refreshDashboard(false);
      reconnectWalletSocket();
      setMessage(`Import finished: ${result.added} added, ${result.updated} updated, ${result.invalid.length} invalid.`, "success");
    } catch (error) {
      setMessage(error.message, "error");
    }
  });

  document.getElementById("refresh-button").addEventListener("click", () => refreshDashboard());
  document.getElementById("wallet-search").addEventListener("input", () => {
    renderWalletTable();
    renderWalletDetails();
  });
  document.getElementById("discover-toggle").addEventListener("click", () => {
    if (state.live.discoveryRunning) {
      stopDiscoverySocket();
      setMessage("Discovery feed stopped.", "neutral");
    } else {
      startDiscoverySocket();
      setMessage("Discovery feed started.", "success");
    }
  });
  document.getElementById("scan-discovered").addEventListener("click", scoreDiscoveredCandidates);
}

async function init() {
  installFormHandlers();
  await loadHealth();
  await Promise.all([loadWallets(), loadMarkets()]);
  await refreshDashboard(false);
  reconnectWalletSocket();
  updateDiscoveryCounters();
}

init();
