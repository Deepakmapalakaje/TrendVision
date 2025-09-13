// Clean Trading Dashboard JavaScript
let currentInterval = '1min';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
  setupEventListeners();
  addScrollToBottomButton();
  fetchAndRender();
  updatePipelineStatus();
  setInterval(fetchAndRender, 3000);
  setInterval(updatePipelineStatus, 10000); // Update status every 10 seconds
});

function setupEventListeners() {
  // Interval tabs
  document.querySelectorAll('.tab').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentInterval = btn.dataset.interval;
      fetchAndRender();
    });
  });

  // Clear button
  document.getElementById('clear-btn').addEventListener('click', () => {
    document.getElementById('market-feed').innerHTML = '';
  });
}

function fetchAndRender() {
  fetch(`/api/summary?interval=${currentInterval}`)
    .then(r => r.json())
    .then(d => {
      if (!d.ok) return;
      renderMarketFeed(d.candles);
      renderTrend(d.trend);
      renderPnl(d.pnl);
      renderMarketSummary(d.market_summary);
      renderPerformanceMetrics(d.performance_metrics);
    })
    .catch(() => {});
}

function renderMarketFeed(candles) {
  const feed = document.getElementById('market-feed');
  
  // Store scroll position before updates
  const wasScrolledToBottom = isScrolledToBottom(feed);
  const scrollTop = feed.scrollTop;
  
  // Create a map of existing messages by instrument_key for efficient updates
  const existingMessages = new Map();
  Array.from(feed.children).forEach(child => {
    const instrumentKey = child.dataset.instrumentKey;
    if (instrumentKey) {
      existingMessages.set(instrumentKey, child);
    }
  });

  // Update or create messages for each candle
  candles.forEach(candle => {
    const instrumentKey = candle.instrument_key;
    let messageElement = existingMessages.get(instrumentKey);
    
    if (messageElement) {
      // Update existing message in place
      updateMessageContent(messageElement, candle);
    } else {
      // Create new message
      messageElement = createMessage(candle);
      messageElement.dataset.instrumentKey = instrumentKey;
      feed.appendChild(messageElement);
      existingMessages.set(instrumentKey, messageElement);
    }
  });

  // Keep only last 20 messages (remove oldest if needed)
  while (feed.children.length > 20) {
    const oldestChild = feed.firstChild;
    const instrumentKey = oldestChild.dataset.instrumentKey;
    if (instrumentKey) {
      existingMessages.delete(instrumentKey);
    }
    feed.removeChild(oldestChild);
  }
  
  // Restore scroll position or auto-scroll to bottom
  if (wasScrolledToBottom) {
    feed.scrollTop = feed.scrollHeight;
  } else {
    feed.scrollTop = scrollTop;
  }
}

function isScrolledToBottom(element) {
  return element.scrollTop + element.clientHeight >= element.scrollHeight - 10;
}

function addScrollToBottomButton() {
  const feed = document.getElementById('market-feed');
  if (!feed) return;
  
  // Create scroll to bottom button
  const scrollButton = document.createElement('button');
  scrollButton.id = 'scroll-to-bottom';
  scrollButton.innerHTML = '↓';
  scrollButton.className = 'scroll-to-bottom-btn';
  scrollButton.title = 'Scroll to bottom';
  
  // Add button to the panel header
  const panelActions = document.querySelector('.panel-actions');
  if (panelActions) {
    panelActions.appendChild(scrollButton);
  }
  
  // Add click event
  scrollButton.addEventListener('click', () => {
    feed.scrollTo({
      top: feed.scrollHeight,
      behavior: 'smooth'
    });
  });
  
  // Show/hide button based on scroll position
  feed.addEventListener('scroll', () => {
    if (isScrolledToBottom(feed)) {
      scrollButton.style.display = 'none';
    } else {
      scrollButton.style.display = 'flex';
    }
  });
  
  // Initially hide the button
  scrollButton.style.display = 'none';
}

function createMessage(candle) {
  const div = document.createElement('div');
  div.className = 'market-message enhanced';
  
  const ts = new Date(candle.timestamp).toLocaleTimeString([], {
    hour: '2-digit', 
    minute: '2-digit',
    second: '2-digit'
  });
  
  const name = candle.instrument_name || candle.symbol || candle.instrument_key;
  
  // Trend indicator
  let trendClass = 'trend-neutral';
  let trendText = 'NEUTRAL';
  if (candle.trend_value === 1) {
    trendClass = 'trend-up';
    trendText = 'UP';
  } else if (candle.trend_value === -1) {
    trendClass = 'trend-down';
    trendText = 'DOWN';
  }
  
  // Price change styling
  const priceChangeClass = candle.price_change_color || 'neutral';
  const deltaClass = candle.delta_color || 'neutral';
  
  // Recommendation
  let recommendation = '';
  if (candle.buy_recommendation) {
    recommendation = `<div class="recommendation">${candle.buy_recommendation}</div>`;
  }
  
  // Trade details if available
  let tradeDetails = '';
  if (candle.entry_price || candle.target || candle.sl) {
    tradeDetails = `
      <div class="trade-details">
        <div class="trade-row">
          <span class="trade-label">Entry:</span>
          <span class="trade-value">${candle.entry_price ? candle.entry_price.toFixed(2) : '-'}</span>
        </div>
        <div class="trade-row">
          <span class="trade-label">Target:</span>
          <span class="trade-value">${candle.target ? candle.target.toFixed(2) : '-'}</span>
        </div>
        <div class="trade-row">
          <span class="trade-label">SL:</span>
          <span class="trade-value">${candle.sl ? candle.sl.toFixed(2) : '-'}</span>
        </div>
        ${candle.profit_loss ? `
        <div class="trade-row">
          <span class="trade-label">P&L:</span>
          <span class="trade-value ${candle.profit_loss > 0 ? 'profit' : 'loss'}">₹${candle.profit_loss.toLocaleString()}</span>
        </div>
        ` : ''}
      </div>
    `;
  }
  
  div.innerHTML = `
    <div class="message-header">
      <div class="instrument-name">${name}</div>
      <div class="timestamp">${ts}</div>
    </div>
    <div class="trend-indicator ${trendClass}">${trendText}</div>
    <div class="price-grid">
      <div class="price-item">
        <div class="price-label">Open</div>
        <div class="price-value">${candle.open.toFixed(2)}</div>
      </div>
      <div class="price-item">
        <div class="price-label">High</div>
        <div class="price-value">${candle.high.toFixed(2)}</div>
      </div>
      <div class="price-item">
        <div class="price-label">Low</div>
        <div class="price-value">${candle.low.toFixed(2)}</div>
      </div>
      <div class="price-item">
        <div class="price-label">Close</div>
        <div class="price-value close">${candle.close.toFixed(2)}</div>
      </div>
    </div>
    <div class="enhanced-stats">
      <div class="stats-row">
        <span class="stat-label">Volume</span>
        <span class="stat-value">${candle.volume_formatted || (candle.volume || 0).toLocaleString()}</span>
      </div>
      <div class="stats-row">
        <span class="stat-label">Price Change</span>
        <span class="stat-value ${priceChangeClass}">${candle.price_change_formatted || '0.00'} (${candle.price_change_pct_formatted || '0.00%'})</span>
      </div>
      ${candle.delta !== undefined ? `
      <div class="stats-row">
        <span class="stat-label">Delta</span>
        <span class="stat-value ${deltaClass}">${candle.delta_formatted || candle.delta} (${candle.delta_pct_formatted || '0.00%'})</span>
      </div>
      ` : ''}
      ${candle.vwap ? `
      <div class="stats-row">
        <span class="stat-label">VWAP</span>
        <span class="stat-value">${candle.vwap.toFixed(2)}</span>
      </div>
      ` : ''}
      ${candle.tick_count ? `
      <div class="stats-row">
        <span class="stat-label">Ticks</span>
        <span class="stat-value">${candle.tick_count}</span>
      </div>
      ` : ''}
    </div>
    ${tradeDetails}
    ${recommendation}
  `;
  
  return div;
}

function updateMessageContent(messageElement, candle) {
  // Store previous values for comparison
  const previousValues = messageElement.dataset.previousValues ? 
    JSON.parse(messageElement.dataset.previousValues) : {};
  
  // Update timestamp
  const timestampElement = messageElement.querySelector('.timestamp');
  if (timestampElement) {
    const ts = new Date(candle.timestamp).toLocaleTimeString([], {
      hour: '2-digit', 
      minute: '2-digit',
      second: '2-digit'
    });
    timestampElement.textContent = ts;
  }

  // Update trend indicator
  const trendElement = messageElement.querySelector('.trend-indicator');
  if (trendElement) {
    let trendClass = 'trend-neutral';
    let trendText = 'NEUTRAL';
    if (candle.trend_value === 1) {
      trendClass = 'trend-up';
      trendText = 'UP';
    } else if (candle.trend_value === -1) {
      trendClass = 'trend-down';
      trendText = 'DOWN';
    }
    trendElement.className = `trend-indicator ${trendClass}`;
    trendElement.textContent = trendText;
  }

  // Helper function to update value with visual feedback
  function updateValue(element, newValue, key) {
    const oldValue = previousValues[key];
    if (oldValue !== newValue) {
      element.textContent = newValue;
      element.classList.add('updated');
      setTimeout(() => element.classList.remove('updated'), 1000);
    }
  }

  // Update price values
  const priceValues = messageElement.querySelectorAll('.price-value');
  priceValues.forEach(priceValue => {
    const label = priceValue.parentElement.querySelector('.price-label').textContent;
    let newValue;
    switch(label) {
      case 'Open':
        newValue = candle.open.toFixed(2);
        updateValue(priceValue, newValue, 'open');
        break;
      case 'High':
        newValue = candle.high.toFixed(2);
        updateValue(priceValue, newValue, 'high');
        break;
      case 'Low':
        newValue = candle.low.toFixed(2);
        updateValue(priceValue, newValue, 'low');
        break;
      case 'Close':
        newValue = candle.close.toFixed(2);
        updateValue(priceValue, newValue, 'close');
        break;
    }
  });

  // Update enhanced stats
  const statsRows = messageElement.querySelectorAll('.enhanced-stats .stats-row');
  statsRows.forEach(row => {
    const label = row.querySelector('.stat-label').textContent;
    const valueElement = row.querySelector('.stat-value');
    
    switch(label) {
      case 'Volume':
        const volumeValue = candle.volume_formatted || (candle.volume || 0).toLocaleString();
        updateValue(valueElement, volumeValue, 'volume');
        break;
      case 'Price Change':
        const priceChangeClass = candle.price_change_color || 'neutral';
        valueElement.className = `stat-value ${priceChangeClass}`;
        const priceChangeValue = `${candle.price_change_formatted || '0.00'} (${candle.price_change_pct_formatted || '0.00%'})`;
        updateValue(valueElement, priceChangeValue, 'priceChange');
        break;
      case 'Delta':
        const deltaClass = candle.delta_color || 'neutral';
        valueElement.className = `stat-value ${deltaClass}`;
        const deltaValue = `${candle.delta_formatted || candle.delta} (${candle.delta_pct_formatted || '0.00%'})`;
        updateValue(valueElement, deltaValue, 'delta');
        break;
      case 'VWAP':
        const vwapValue = candle.vwap ? candle.vwap.toFixed(2) : '0.00';
        updateValue(valueElement, vwapValue, 'vwap');
        break;
      case 'Ticks':
        const tickValue = candle.tick_count || 0;
        updateValue(valueElement, tickValue, 'ticks');
        break;
    }
  });

  // Update trade details if available
  const tradeDetailsElement = messageElement.querySelector('.trade-details');
  if (candle.entry_price || candle.target || candle.sl) {
    if (!tradeDetailsElement) {
      // Create trade details if they don't exist
      const tradeDetails = document.createElement('div');
      tradeDetails.className = 'trade-details';
      tradeDetails.innerHTML = `
        <div class="trade-row">
          <span class="trade-label">Entry:</span>
          <span class="trade-value">${candle.entry_price ? candle.entry_price.toFixed(2) : '-'}</span>
        </div>
        <div class="trade-row">
          <span class="trade-label">Target:</span>
          <span class="trade-value">${candle.target ? candle.target.toFixed(2) : '-'}</span>
        </div>
        <div class="trade-row">
          <span class="trade-label">SL:</span>
          <span class="trade-value">${candle.sl ? candle.sl.toFixed(2) : '-'}</span>
        </div>
        ${candle.profit_loss ? `
        <div class="trade-row">
          <span class="trade-label">P&L:</span>
          <span class="trade-value ${candle.profit_loss > 0 ? 'profit' : 'loss'}">₹${candle.profit_loss.toLocaleString()}</span>
        </div>
        ` : ''}
      `;
      messageElement.appendChild(tradeDetails);
    } else {
      // Update existing trade details
      const tradeRows = tradeDetailsElement.querySelectorAll('.trade-row');
      tradeRows.forEach(row => {
        const label = row.querySelector('.trade-label').textContent;
        const valueElement = row.querySelector('.trade-value');
        
        switch(label) {
          case 'Entry:':
            valueElement.textContent = candle.entry_price ? candle.entry_price.toFixed(2) : '-';
            break;
          case 'Target:':
            valueElement.textContent = candle.target ? candle.target.toFixed(2) : '-';
            break;
          case 'SL:':
            valueElement.textContent = candle.sl ? candle.sl.toFixed(2) : '-';
            break;
          case 'P&L:':
            if (candle.profit_loss) {
              valueElement.textContent = `₹${candle.profit_loss.toLocaleString()}`;
              valueElement.className = `trade-value ${candle.profit_loss > 0 ? 'profit' : 'loss'}`;
            }
            break;
        }
      });
    }
  }

  // Update recommendation
  const recommendationElement = messageElement.querySelector('.recommendation');
  if (candle.buy_recommendation) {
    if (!recommendationElement) {
      const recommendation = document.createElement('div');
      recommendation.className = 'recommendation';
      recommendation.textContent = candle.buy_recommendation;
      messageElement.appendChild(recommendation);
    } else {
      recommendationElement.textContent = candle.buy_recommendation;
    }
  } else if (recommendationElement) {
    recommendationElement.remove();
  }

  // Store current values for next comparison
  const currentValues = {
    open: candle.open.toFixed(2),
    high: candle.high.toFixed(2),
    low: candle.low.toFixed(2),
    close: candle.close.toFixed(2),
    volume: candle.volume_formatted || (candle.volume || 0).toLocaleString(),
    priceChange: `${candle.price_change_formatted || '0.00'} (${candle.price_change_pct_formatted || '0.00%'})`,
    delta: `${candle.delta_formatted || candle.delta} (${candle.delta_pct_formatted || '0.00%'})`,
    vwap: candle.vwap ? candle.vwap.toFixed(2) : '0.00',
    ticks: candle.tick_count || 0
  };
  
  messageElement.dataset.previousValues = JSON.stringify(currentValues);
}

function renderTrend(trend) {
  const trendValue = document.querySelector('.trend-value');
  const trendTime = document.getElementById('trend-time');
  
  if (trend.trend_value === 1) {
    trendValue.textContent = 'UP';
    trendValue.style.color = 'var(--accent)';
  } else if (trend.trend_value === -1) {
    trendValue.textContent = 'DOWN';
    trendValue.style.color = 'var(--danger)';
  } else {
    trendValue.textContent = 'NEUTRAL';
    trendValue.style.color = 'var(--text)';
  }
  
  if (trend.timestamp) {
    const time = new Date(trend.timestamp).toLocaleTimeString([], {
      hour: '2-digit', 
      minute: '2-digit'
    });
    trendTime.textContent = time;
  }
}

function renderPnl(pnl) {
  document.getElementById('profit').textContent = `₹${pnl.profit.toLocaleString()}`;
  document.getElementById('loss').textContent = `₹${pnl.loss.toLocaleString()}`;
  
  const net = pnl.profit - pnl.loss;
  const netElement = document.getElementById('net');
  netElement.textContent = `₹${net.toLocaleString()}`;
  
  if (net > 0) {
    netElement.style.color = 'var(--accent)';
  } else if (net < 0) {
    netElement.style.color = 'var(--danger)';
  } else {
    netElement.style.color = 'var(--text)';
  }
}

function renderMarketSummary(summary) {
  if (!summary) return;
  
  // Create or update market summary display
  let summaryElement = document.getElementById('market-summary');
  if (!summaryElement) {
    summaryElement = document.createElement('div');
    summaryElement.id = 'market-summary';
    summaryElement.className = 'card market-summary-card';
    summaryElement.innerHTML = `
      <div class="card-header">
        <h4>Market Summary</h4>
      </div>
      <div class="card-content">
        <div class="summary-grid" id="summary-grid"></div>
      </div>
    `;
    
    // Insert after trend card
    const trendCard = document.querySelector('.card');
    trendCard.parentNode.insertBefore(summaryElement, trendCard.nextSibling);
  }
  
  const summaryGrid = document.getElementById('summary-grid');
  const sentimentClass = summary.market_sentiment === 'BULLISH' ? 'positive' : 
                        summary.market_sentiment === 'BEARISH' ? 'negative' : 'neutral';
  
  // Update values in place
  updateSummaryValue(summaryGrid, 'Instruments', summary.total_instruments || 0);
  updateSummaryValue(summaryGrid, 'Total Volume', (summary.total_volume || 0).toLocaleString());
  updateSummaryValue(summaryGrid, 'Avg Change', `${(summary.avg_price_change_pct || 0).toFixed(2)}%`);
  updateSummaryValue(summaryGrid, 'Sentiment', summary.market_sentiment || 'NEUTRAL', sentimentClass);
  updateSummaryValue(summaryGrid, 'Positive', summary.positive_moves || 0, 'positive');
  updateSummaryValue(summaryGrid, 'Negative', summary.negative_moves || 0, 'negative');
}

function updateSummaryValue(container, label, value, className = '') {
  let item = Array.from(container.children).find(child => 
    child.querySelector('.summary-label').textContent === label
  );
  
  if (!item) {
    item = document.createElement('div');
    item.className = 'summary-item';
    item.innerHTML = `
      <div class="summary-label">${label}</div>
      <div class="summary-value ${className}">${value}</div>
    `;
    container.appendChild(item);
  } else {
    const valueElement = item.querySelector('.summary-value');
    const oldValue = valueElement.textContent;
    if (oldValue !== value) {
      valueElement.textContent = value;
      valueElement.className = `summary-value ${className}`;
      valueElement.classList.add('updated');
      setTimeout(() => valueElement.classList.remove('updated'), 1000);
    }
  }
}

function renderPerformanceMetrics(metrics) {
  if (!metrics) return;
  
  // Create or update performance metrics display
  let metricsElement = document.getElementById('performance-metrics');
  if (!metricsElement) {
    metricsElement = document.createElement('div');
    metricsElement.id = 'performance-metrics';
    metricsElement.className = 'card performance-metrics-card';
    metricsElement.innerHTML = `
      <div class="card-header">
        <h4>Performance Metrics</h4>
      </div>
      <div class="card-content">
        <div class="metrics-grid" id="metrics-grid"></div>
      </div>
    `;
    
    // Insert after market summary
    const summaryCard = document.getElementById('market-summary');
    if (summaryCard) {
      summaryCard.parentNode.insertBefore(metricsElement, summaryCard.nextSibling);
    }
  }
  
  const metricsGrid = document.getElementById('metrics-grid');
  
  // Update values in place
  updateMetricValue(metricsGrid, 'Total Delta', (metrics.total_delta || 0).toLocaleString());
  updateMetricValue(metricsGrid, 'Avg Delta', (metrics.avg_delta || 0).toFixed(2));
  updateMetricValue(metricsGrid, 'Max Change', `${(metrics.max_price_change || 0).toFixed(2)}%`);
  updateMetricValue(metricsGrid, 'Min Change', `${(metrics.min_price_change || 0).toFixed(2)}%`);
  updateMetricValue(metricsGrid, 'Total Ticks', (metrics.total_tick_count || 0).toLocaleString());
  updateMetricValue(metricsGrid, 'Avg Ticks', (metrics.avg_tick_count || 0).toFixed(0));
}

function updateMetricValue(container, label, value) {
  let item = Array.from(container.children).find(child => 
    child.querySelector('.metric-label').textContent === label
  );
  
  if (!item) {
    item = document.createElement('div');
    item.className = 'metric-item';
    item.innerHTML = `
      <div class="metric-label">${label}</div>
      <div class="metric-value">${value}</div>
    `;
    container.appendChild(item);
  } else {
    const valueElement = item.querySelector('.metric-value');
    const oldValue = valueElement.textContent;
    if (oldValue !== value) {
      valueElement.textContent = value;
      valueElement.classList.add('updated');
      setTimeout(() => valueElement.classList.remove('updated'), 1000);
    }
  }
}

function updatePipelineStatus() {
  fetch('/api/status')
    .then(r => r.json())
    .then(data => {
      const statusElement = document.getElementById('pipeline-status');
      const statusText = statusElement.querySelector('.status-text');
      const statusDot = statusElement.querySelector('.status-dot');
      
      if (data.pipeline_running) {
        statusText.textContent = 'LIVE';
        statusDot.style.background = 'var(--accent)';
      } else if (data.market_hours) {
        statusText.textContent = 'STARTING';
        statusDot.style.background = 'var(--warning)';
      } else {
        statusText.textContent = 'CLOSED';
        statusDot.style.background = 'var(--text-muted)';
      }
    })
    .catch(() => {});
}

// Legacy functions for backward compatibility
function renderCandles(candles) {
  renderMarketFeed(candles);
}