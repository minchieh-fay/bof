package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"gopkg.in/yaml.v3"
)

const bollPeriod = 20

// config 定义生产买入策略及账户参数。
type config struct {
	KeyFile      string  `yaml:"keyfile"`
	Interval     string  `yaml:"kline_interval"`
	Amount       float64 `yaml:"amount"`
	Leverage     int     `yaml:"leverage"`
	AmountStep   float64 `yaml:"amount_step"`
	InitialDrop  float64 `yaml:"initial_drop"`
	DropStep     float64 `yaml:"drop_step"`
	DecayMinutes int     `yaml:"decay_minutes"`
	PositionMode string  `yaml:"position_mode"`
	RankFraction float64 `yaml:"rank_fraction"`
}

// help_loadConfig 载入带有默认值的 YAML 配置并检查策略参数。
func help_loadConfig(path string) (config, error) {
	// step.1 用 YAML 文件覆写默认配置，拒绝未知字段。
	cfg := config{"/root/key", "30m", 100, 5, 0.5, 0.3, 0.2, 60, "hedge", 0.8}
	f, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer f.Close()
	decoder := yaml.NewDecoder(f)
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	// step.2 校验合约周期、金额、杠杆、递减和持仓参数。
	intervals := []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h",
		"12h", "1d", "3d", "1w", "1M"}
	if !slices.Contains(intervals, cfg.Interval) || strings.TrimSpace(cfg.KeyFile) == "" ||
		cfg.Leverage < 1 || cfg.Leverage > 125 || cfg.DecayMinutes < 1 || cfg.DecayMinutes > 525600 ||
		(cfg.PositionMode != "hedge" && cfg.PositionMode != "one_way") {
		return cfg, errors.New("周期、凭据路径、杠杆、递减分钟数或持仓模式无效")
	}
	for _, n := range []float64{cfg.Amount, cfg.AmountStep, cfg.InitialDrop, cfg.DropStep, cfg.RankFraction} {
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
			return cfg, errors.New("策略金额和比例必须是有限的非负数")
		}
	}
	if cfg.Amount == 0 || cfg.InitialDrop >= 1 || cfg.RankFraction <= 0 || cfg.RankFraction > 1 {
		return cfg, errors.New("amount 必须大于 0，initial_drop 必须小于 1，rank_fraction 必须在 (0, 1] 内")
	}
	return cfg, nil
}

// help_readKeys 从凭据文件前两行读取 API Key 和 Secret。
func help_readKeys(path string) (string, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	lines := strings.SplitN(string(data), "\n", 3)
	if len(lines) < 2 || strings.TrimSpace(lines[0]) == "" || strings.TrimSpace(lines[1]) == "" {
		return "", "", errors.New("凭据文件前两行必须分别填写 API Key 和 Secret")
	}
	return strings.TrimSpace(lines[0]), strings.TrimSpace(lines[1]), nil
}

// help_positive 将行情中的十进制字符串转换为有效正数。
func help_positive(value string) float64 {
	n, err := strconv.ParseFloat(value, 64)
	if err != nil || n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) {
		return 0
	}
	return n
}

// help_wait 等待请求间隔或响应服务退出。
func help_wait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// help_prepareAccount 校验实盘账户并清理本策略的遗留挂单。
func (b *bot) help_prepareAccount(ctx context.Context) error {
	// step.1 对齐签名时间并验证账户的实际持仓模式。
	if _, err := b.futures.NewSetServerTimeService().Do(ctx); err != nil {
		return fmt.Errorf("同步合约服务器时间: %w", err)
	}
	mode, err := b.futures.NewGetPositionModeService().Do(ctx)
	if err != nil {
		return fmt.Errorf("读取持仓模式: %w", err)
	}
	if mode.DualSidePosition != (b.cfg.PositionMode == "hedge") {
		return errors.New("position_mode 与账户实际持仓模式不匹配")
	}
	// step.2 按客户端订单号清理本策略上次留下的挂单。
	orders, err := b.futures.NewListOpenOrdersService().Do(ctx)
	if err != nil {
		return fmt.Errorf("读取未完成合约订单: %w", err)
	}
	for _, order := range orders {
		if strings.HasPrefix(order.ClientOrderID, "bof_") {
			// 确认遗留买单已结束后才恢复策略。
			if _, ok := b.help_finishOrder(ctx, order.Symbol, order.ClientOrderID); !ok {
				return fmt.Errorf("无法确认遗留订单 %s 已结束", order.ClientOrderID)
			}
		}
	}
	return nil
}

// help_marketRules 读取有同名可交易 USDT 现货的合约规则。
func (b *bot) help_marketRules(ctx context.Context) (map[string]futures.Symbol, error) {
	// step.1 读取现货和合约市场清单。
	spot, err := b.spot.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取现货市场: %w", err)
	}
	// 拉开两次交易所信息查询的请求间隔。
	if !help_wait(ctx, 50*time.Millisecond) {
		return nil, ctx.Err()
	}
	info, err := b.futures.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取合约市场: %w", err)
	}
	// step.2 确定与可交易 USDT 现货匹配的合约订阅范围。
	listed := make(map[string]bool)
	for _, entry := range spot.Symbols {
		if entry.Status == "TRADING" && entry.QuoteAsset == "USDT" && entry.IsSpotTradingAllowed {
			listed[entry.Symbol] = true
		}
	}
	rules := make(map[string]futures.Symbol)
	for _, rule := range info.Symbols {
		if listed[rule.Symbol] && strings.HasSuffix(rule.Symbol, "USDT") &&
			rule.QuoteAsset == "USDT" && rule.Status == "TRADING" {
			rules[rule.Symbol] = rule
		}
	}
	return rules, nil
}

// help_boll 用最多 20 根收盘价计算 2 倍总体标准差的布林下轨。
func help_boll(closes []float64) ([]float64, float64) {
	if len(closes) < bollPeriod {
		return closes, 0
	}
	closes = closes[len(closes)-bollPeriod:]
	var mean, variance float64
	for _, price := range closes {
		mean += price / bollPeriod
	}
	for _, price := range closes {
		variance += (price - mean) * (price - mean) / bollPeriod
	}
	return closes, mean - 2*math.Sqrt(variance)
}

// help_loadHistory 拉取已收盘合约 K 线并更新策略布林带。
func (b *bot) help_loadHistory(ctx context.Context, symbol string, m *market) {
	// step.1 获取可用于布林计算的已收盘价格。
	now := time.Now().UnixMilli()
	klines, err := b.futures.NewKlinesService().Symbol(symbol).Interval(b.cfg.Interval).
		EndTime(now).Limit(bollPeriod + 1).Do(ctx)
	var closes []float64
	var closedAt int64
	for _, k := range klines {
		// 过滤无效收盘价，保证布林窗口只使用有效数据。
		if closePrice := help_positive(k.Close); closePrice > 0 && k.CloseTime < now {
			if closedAt != 0 && k.OpenTime != closedAt+1 {
				closes = nil
			}
			closes, closedAt = append(closes, closePrice), k.CloseTime
		}
	}
	// step.2 提交历史数据并恢复该市场的行情计算资格。
	b.mu.Lock()
	defer b.mu.Unlock()
	m.loading = false
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("%s 历史 K 线获取失败: %v", symbol, err)
		}
		return
	}
	if b.markets[symbol] == m && closedAt >= m.closedAt {
		// 根据恢复的连续收盘价重建下轨。
		m.closes, m.lower = help_boll(closes)
		m.closedAt = closedAt
	}
}

// help_scheduleBackfill 合并行情断档后的 REST 历史补齐请求。
func (b *bot) help_scheduleBackfill(ctx context.Context, symbol string, m *market) {
	if m.loading {
		return
	}
	m.loading = true
	b.jobs.Add(1)
	// 对不同交易对的历史补齐请求统一限速。
	go b.help_backfill(ctx, symbol, m)
}

// help_backfill 通过共享请求间隔重新加载断档市场的历史。
func (b *bot) help_backfill(ctx context.Context, symbol string, m *market) {
	defer b.jobs.Done()
	// 同步限速后补齐该市场的已收盘数据。
	b.help_pacedHistory(ctx, symbol, m)
}

// help_pacedHistory 串行执行历史查询，避免多个市场重连时同时拉取。
func (b *bot) help_pacedHistory(ctx context.Context, symbol string, m *market) {
	b.historyMu.Lock()
	defer b.historyMu.Unlock()
	// 在历史行情请求之间保留 50ms 间隔。
	help_wait(ctx, 50*time.Millisecond)
	// 更新该交易对的完整布林窗口。
	b.help_loadHistory(ctx, symbol, m)
}

// help_spotVolume 获取最近七个已收盘 UTC 日的 USDT 成交额。
func (b *bot) help_spotVolume(ctx context.Context, symbol string) (float64, error) {
	yesterday := time.Now().UTC().Truncate(24*time.Hour).UnixMilli() - 1
	klines, err := b.spot.NewKlinesService().Symbol(symbol).Interval("1d").EndTime(yesterday).Limit(7).Do(ctx)
	if err != nil {
		return 0, err
	}
	var volume float64
	for _, k := range klines {
		// 求和现货日线的计价资产成交额。
		volume += help_positive(k.QuoteAssetVolume)
	}
	return volume, nil
}

// help_refreshData 初始化合约历史并定期轮转现货成交额。
func (b *bot) help_refreshData(ctx context.Context, initial bool) {
	// step.1 为首次出现或历史获取失败的合约补齐 K 线。
	b.mu.Lock()
	names := make([]string, 0, len(b.markets))
	for name := range b.markets {
		names = append(names, name)
	}
	b.mu.Unlock()
	sort.Strings(names)
	if initial {
		log.Printf("正在同步历史 K 线，周期 %s，共 %d 个交易对", b.cfg.Interval, len(names))
	}
	for i, name := range names {
		if ctx.Err() != nil {
			return
		}
		b.mu.Lock()
		m := b.markets[name]
		needsHistory := m.closedAt == 0 && !m.loading
		if needsHistory {
			m.loading = true
		}
		b.mu.Unlock()
		if needsHistory {
			// 用历史收盘价预热合约布林窗口。
			b.help_pacedHistory(ctx, name, m)
		}
		if initial && ctx.Err() == nil {
			// 汇报历史 K 线阶段已处理的交易对数量。
			help_logStartupProgress("历史 K 线同步", i+1, len(names))
		}
	}
	// step.2 首次逐个加载成交额，之后按名称轮转每小时刷新一项。
	if !initial && len(names) > 0 {
		i := sort.SearchStrings(names, b.volumeCursor)
		if i < len(names) && names[i] == b.volumeCursor {
			i++
		}
		names = []string{names[i%len(names)]}
	}
	if initial {
		log.Printf("正在同步现货成交额，共 %d 个交易对", len(names))
	}
	for i, name := range names {
		// 在逐交易对查询现货成交额时保留调用间隔。
		if !help_wait(ctx, 50*time.Millisecond) {
			return
		}
		b.volumeCursor = name
		// 读取该现货近七日成交额。
		v, err := b.help_spotVolume(ctx, name)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("%s 现货成交额获取失败: %v", name, err)
		} else {
			b.mu.Lock()
			b.markets[name].volume = v
			b.mu.Unlock()
		}
		if initial {
			// 汇报现货成交额阶段已处理的交易对数量。
			help_logStartupProgress("现货成交额同步", i+1, len(names))
		}
	}
	// step.3 按更新后的成交额重算允许买入的集合。
	b.mu.Lock()
	// 保留完整订阅集合，仅修改买入权限。
	b.help_rank()
	b.mu.Unlock()
}

// help_logStartupProgress 每处理 20 个交易对以及处理最后一个时记录初始化进度。
func help_logStartupProgress(stage string, processed, total int) {
	if processed%20 == 0 || processed == total {
		log.Printf("%s进度 已处理 %d/%d", stage, processed, total)
	}
}

// help_rank 按成交额从高到低标记前指定比例的交易对。
func (b *bot) help_rank() {
	names := make([]string, 0, len(b.markets))
	for name, m := range b.markets {
		m.ranked = false
		if slices.Contains(m.rules.TimeInForce, futures.TimeInForceTypeGTX) {
			names = append(names, name)
		}
	}
	sort.Slice(names, func(i, j int) bool {
		left, right := b.markets[names[i]].volume, b.markets[names[j]].volume
		if left == right {
			return names[i] < names[j]
		}
		return left > right
	})
	for _, name := range names[:int(math.Ceil(float64(len(names))*b.cfg.RankFraction))] {
		b.markets[name].ranked = b.markets[name].volume > 0
	}
}

// help_observeBoll 记录本轮比例最大的行情，调用方须持有共享状态锁。
func (b *bot) help_observeBoll(symbol string, price, lower float64) {
	if lower <= 0 {
		return
	}
	ratio := 1 - price/lower
	if b.bollExtreme.symbol == "" || ratio > b.bollExtreme.ratio {
		b.bollExtreme = bollObservation{symbol: symbol, price: price, lower: lower, ratio: ratio}
	}
}

// help_reportBoll 每五分钟输出布林偏离摘要并重置下一轮统计。
func (b *bot) help_reportBoll(ctx context.Context) {
	defer b.jobs.Done()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// step.1 一次性读取并清空本轮偏离统计，同时获取全局买入计数。
			b.mu.Lock()
			observation, times := b.bollExtreme, b.times
			b.bollExtreme = bollObservation{}
			b.mu.Unlock()

			// step.2 输出本轮摘要，没有样本时也打印当前买入计数。
			if observation.symbol == "" {
				log.Printf("布林下轨统计 近5分钟无有效行情 g_times=%d", times)
				continue
			}
			log.Printf("布林下轨统计 近5分钟 交易对=%s 价格=%g 下轨=%g 偏离比例=%+.4f%% g_times=%d",
				observation.symbol, observation.price, observation.lower, observation.ratio*100, times)
		}
	}
}

// help_decay 每经过一个完整的配置间隔就扣减一次全局买入计数。
func (b *bot) help_decay(now time.Time) {
	interval := time.Duration(b.cfg.DecayMinutes) * time.Minute
	if b.times == 0 || now.Before(b.decayAt.Add(interval)) {
		return
	}
	elapsed := now.Sub(b.decayAt) / interval
	b.times = max(0, b.times-int(elapsed))
	b.decayAt = b.decayAt.Add(elapsed * interval)
}

// help_streams 创建按最多 100 个合约分组的订阅，并在交易对集合变化时重建。
func (b *bot) help_streams(ctx context.Context) func(bool) {
	var active []string
	var cancel context.CancelFunc
	var workers sync.WaitGroup
	// 更新订阅集合，退出时等待所有行情回调结束。
	update := func(start bool) {
		// step.1 取得需要继续订阅的合约清单。
		var names []string
		if start {
			b.mu.Lock()
			for name := range b.markets {
				names = append(names, name)
			}
			b.mu.Unlock()
			sort.Strings(names)
			if slices.Equal(active, names) {
				return
			}
		}
		// step.2 关闭过期订阅并依新清单建立分组连接。
		if cancel != nil {
			cancel()
			workers.Wait()
		}
		active = names
		var streamCtx context.Context
		streamCtx, cancel = context.WithCancel(ctx)
		for group := range slices.Chunk(names, 100) {
			workers.Add(1)
			// 持续维护本组合约的 WebSocket 行情连接。
			go b.help_streamGroup(streamCtx, group, &workers)
		}
	}
	update(true)
	return update
}

// help_streamGroup 维持单组行情订阅并在网络故障后退避重连。
func (b *bot) help_streamGroup(ctx context.Context, names []string, workers *sync.WaitGroup) {
	defer workers.Done()
	// step.1 按本组合约构造实时 K 线订阅。
	streams := make([]string, 0, len(names))
	for _, name := range names {
		streams = append(streams, strings.ToLower(name)+"@kline_"+b.cfg.Interval)
	}
	endpoint := "wss://fstream.binance.com/market/stream?streams=" + strings.Join(streams, "/")
	// step.2 维持行情连接并在断开时退避重连。
	delay := time.Second
	for ctx.Err() == nil {
		// 读取一轮实时行情，连接中断后返回重连。
		err := b.help_readStream(ctx, endpoint)
		if ctx.Err() == nil {
			log.Printf("行情连接 %s 等待重连: %v", names[0], err)
		}
		// 避免在交易所或网络故障期间立即反复拨号。
		if !help_wait(ctx, delay) {
			return
		}
		delay = min(delay*2, 30*time.Second)
	}
}

// help_readStream 读取一条 WebSocket 连接上的合约 K 线事件。
func (b *bot) help_readStream(ctx context.Context, endpoint string) error {
	// step.1 建立行情连接并在服务退出时解除读取等待。
	conn, response, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		if response != nil {
			response.Body.Close()
		}
		return err
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	conn.SetReadLimit(1 << 20)
	// step.2 转发合约 K 线，长时间无行情时触发重连。
	for ctx.Err() == nil {
		var envelope struct {
			Data futures.WsKlineEvent `json:"data"`
		}
		if err := conn.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
			return err
		}
		if err := conn.ReadJSON(&envelope); err != nil {
			return err
		}
		if envelope.Data.Event == "kline" {
			b.onKline(ctx, &envelope.Data)
		}
	}
	return ctx.Err()
}

// help_apiCode 提取 Binance 返回的错误码。
func help_apiCode(err error) int64 {
	var apiErr *common.APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return 0
}

// help_orderID 生成满足 Binance 长度和字符约束的客户端订单号。
func help_orderID(symbol string) (string, error) {
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	var name strings.Builder
	for _, r := range symbol {
		if r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			name.WriteRune(r)
		}
	}
	return "bof_" + name.String()[:min(name.Len(), 19)] + "_" + hex.EncodeToString(suffix[:]), nil
}

// help_decimal 解析交易所或结算返回的数据。
func help_decimal(value string) decimal.Decimal {
	n, _ := decimal.NewFromString(value)
	return n
}

// help_buyQuantity 依据信号价格及全局次数换算合约数量，杠杆仅影响保证金。
func help_buyQuantity(cfg config, price string, times int) decimal.Decimal {
	factor := decimal.NewFromFloat(cfg.AmountStep).Mul(decimal.NewFromInt(int64(times))).Add(decimal.NewFromInt(1))
	// 将行情字符串解析为精确十进制价格。
	return decimal.NewFromFloat(cfg.Amount).Mul(factor).DivRound(help_decimal(price), 24)
}

// help_orderMarket 只为仍符合触发价的实时行情返回交易规则和报价。
func (b *bot) help_orderMarket(symbol string, threshold float64) (futures.Symbol, string, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.markets[symbol]
	if m == nil || !m.ranked || m.loading || m.price == "" || time.Since(time.UnixMilli(m.priceAt)) > 10*time.Second {
		return futures.Symbol{}, "", false
	}
	// 将实时成交价与本次买入信号的触发价比较。
	price := help_positive(m.price)
	if price <= 0 || price >= threshold {
		return futures.Symbol{}, "", false
	}
	return m.rules, m.price, true
}

// help_orderTerms 将价格和剩余数量向下对齐并检查单笔订单限制。
func help_orderTerms(rule futures.Symbol, latest string, remaining decimal.Decimal, attempt int64) (string, string, bool) {
	// step.1 将目标价格和数量对齐到交易所要求的档位。
	prices, lots := rule.PriceFilter(), rule.LotSizeFilter()
	if prices == nil || lots == nil {
		return "", "", false
	}
	// 将交易所最小变动单位和最新价格转换为十进制数。
	tick, step, price := help_decimal(prices.TickSize), help_decimal(lots.StepSize), help_decimal(latest)
	if !tick.IsPositive() || !step.IsPositive() || !price.IsPositive() {
		return "", "", false
	}
	ticks, _ := price.QuoRem(tick, 0)
	price = ticks.Sub(decimal.NewFromInt(attempt)).Mul(tick)
	// 按单笔最大数量裁剪本次挂单。
	units, _ := decimal.Min(remaining, help_decimal(lots.MaxQuantity)).QuoRem(step, 0)
	qty := units.Mul(step)

	// step.2 验证价格、数量和最小名义金额。
	// 读取交易所规定的价格上下限。
	minPrice, maxPrice := help_decimal(prices.MinPrice), help_decimal(prices.MaxPrice)
	// 读取限价单最小交易数量。
	minQty := help_decimal(lots.MinQuantity)
	if !price.IsPositive() || price.LessThan(minPrice) || (maxPrice.IsPositive() && price.GreaterThan(maxPrice)) ||
		!qty.IsPositive() || qty.LessThan(minQty) {
		return "", "", false
	}
	if minNotional := rule.MinNotionalFilter(); minNotional != nil {
		// 检查当前报价乘以数量是否达到最小名义金额。
		if price.Mul(qty).LessThan(help_decimal(minNotional.Notional)) {
			return "", "", false
		}
	}
	return price.String(), qty.String(), true
}

// help_postBuy 仅在 GTX 价格冲突时逐档降价，单轮最多提交三次。
func (b *bot) help_postBuy(ctx context.Context, symbol string, threshold float64, remaining decimal.Decimal) (
	id string, accepted, uncertain, retry bool,
) {
	for attempt := int64(0); attempt < 3 && ctx.Err() == nil; attempt++ {
		// step.1 根据合约交易规则准备该档位的 GTX 买入委托。
		// 每一档都重新读取行情，价格回到触发价时结束本次买入。
		rule, latest, ok := b.help_orderMarket(symbol, threshold)
		if !ok {
			return "", false, false, false
		}
		// 计算可提交的价格和剩余数量。
		price, qty, ok := help_orderTerms(rule, latest, remaining, attempt)
		if !ok {
			log.Printf("%s 剩余买入数量 %s 或报价不满足交易限制", rule.Symbol, remaining)
			return "", false, false, false
		}
		// 生成用于下单及后续状态核对的客户端订单号。
		id, err := help_orderID(rule.Symbol)
		if err != nil {
			log.Printf("%s 生成客户端订单号失败: %v", rule.Symbol, err)
			return "", false, false, false
		}
		order := b.futures.NewCreateOrderService().Symbol(rule.Symbol).Side(futures.SideTypeBuy).
			Type(futures.OrderTypeLimit).TimeInForce(futures.TimeInForceTypeGTX).
			Price(price).Quantity(qty).NewClientOrderID(id)
		if b.cfg.PositionMode == "hedge" {
			order.PositionSide(futures.PositionSideTypeLong)
		}

		// step.2 提交订单，根据结果选择结算、查单或继续降价。
		if _, err = order.Do(ctx); err == nil {
			return id, true, false, false
		}
		// 区分价格冲突、结果不明和确定拒绝。
		code := help_apiCode(err)
		if code != -5022 {
			log.Printf("%s 提交买单 %s: %v", rule.Symbol, id, err)
			switch code {
			case 0, -1000, -1001, -1006, -1007:
				return id, false, true, false
			default:
				return "", false, false, false
			}
		}
		if attempt < 2 {
			// 价格冲突后间隔 50ms 再降一档。
			help_wait(ctx, 50*time.Millisecond)
		}
	}
	return "", false, false, ctx.Err() == nil
}

// help_isTerminal 判断订单是否已经结束且不会再成交。
func help_isTerminal(status futures.OrderStatusType) bool {
	switch status {
	case "FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH":
		return true
	default:
		return false
	}
}

// help_finishOrder 撤销剩余订单并以最终状态核对累计成交量。
func (b *bot) help_finishOrder(ctx context.Context, symbol, id string) (decimal.Decimal, bool) {
	// step.1 在有限时间内对账，退出信号到达后仍完成在途买单清理。
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	var filled decimal.Decimal
	for cleanup.Err() == nil {
		// step.2 先撤销挂单，再通过查单处理成交与撤单交错或结果不明。
		result, err := b.futures.NewCancelOrderService().Symbol(symbol).OrigClientOrderID(id).Do(cleanup)
		// 只有交易所确认订单进入终态时才允许重新挂单。
		if err == nil && help_isTerminal(result.Status) {
			// 撤单回执包含订单结束前的累计成交量。
			return help_decimal(result.ExecutedQuantity), true
		}
		// 分隔撤单与后续状态查询。
		if !help_wait(cleanup, 50*time.Millisecond) {
			break
		}
		order, err := b.futures.NewGetOrderService().Symbol(symbol).OrigClientOrderID(id).Do(cleanup)
		if err == nil {
			// 即使订单尚未结束，也保留已经查到的累计成交量。
			filled = help_decimal(order.ExecutedQuantity)
			// 检查查单回执是否已进入终态。
			if help_isTerminal(order.Status) {
				return filled, true
			}
		}
		// 订单状态仍待确认时避免连续请求交易所。
		help_wait(cleanup, time.Second)
	}
	return filled, false
}
