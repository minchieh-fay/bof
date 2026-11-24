package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	binance "github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/futures"
)

// market 保存单个合约的交易规则、行情和现货排名数据。
type market struct {
	rules    futures.Symbol
	closes   []float64
	lower    float64
	closedAt int64
	price    string
	priceAt  int64
	volume   float64
	ranked   bool
	loading  bool
}

// bollObservation 保存本轮行情中价格相对布林下轨偏离比例最大的样本。
type bollObservation struct {
	symbol string
	price  float64
	lower  float64
	ratio  float64
}

// bot 管理策略共享状态及交易所连接。
type bot struct {
	ctx          context.Context
	cfg          config
	futures      *futures.Client
	spot         *binance.Client
	mu           sync.Mutex
	historyMu    sync.Mutex
	volumeCursor string
	markets      map[string]*market
	busy         map[string]bool
	times        int
	decayAt      time.Time
	bollExtreme  bollObservation
	jobs         sync.WaitGroup
}

// main 读取配置并运行实盘买入服务。
func main() {
	// step.1 加载运行配置和实盘凭据。
	path := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()
	log.Printf("BOF 正在启动，读取配置 %s", *path)
	// 读取并校验买入策略配置。
	cfg, err := help_loadConfig(*path)
	if err != nil {
		log.Fatal(err)
	}
	// 从独立文件读取真实交易账户凭据。
	key, secret, err := help_readKeys(cfg.KeyFile)
	if err != nil {
		log.Fatal(err)
	}

	// step.2 建立交易客户端并在收到退出信号后清理挂单。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client := futures.NewClient(key, secret)
	client.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	spot := binance.NewClient("", "")
	spot.HTTPClient = client.HTTPClient
	b := &bot{ctx: ctx, cfg: cfg, futures: client, spot: spot, markets: make(map[string]*market),
		busy: make(map[string]bool), decayAt: time.Now()}
	if err := b.run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

// run 初始化市场和账户，并定时维护订阅、现货排行及买入计数。
func (b *bot) run(ctx context.Context) error {
	// step.1 校验账户并恢复上次进程遗留的策略挂单。
	log.Print("正在校验币安合约账户并检查遗留挂单")
	// 对齐签名时间、验证持仓模式并撤销遗留挂单。
	if err := b.help_prepareAccount(ctx); err != nil {
		return err
	}
	// step.2 初始化市场集合、历史行情和成交额排行，再启动分组订阅。
	log.Print("正在读取币安现货和合约交易对清单")
	if err := b.refresh(ctx, true); err != nil {
		return err
	}
	log.Printf("市场初始化完成，订阅 %d 个 USDT 合约，周期 %s", len(b.markets), b.cfg.Interval)
	// 根据当前合约集合建立行情订阅。
	restart := b.help_streams(ctx)
	b.jobs.Add(1)
	// 独立记录五分钟行情摘要，避免市场刷新延迟日志。
	go b.help_reportBoll(ctx)
	refresh := time.NewTicker(time.Hour)
	decay := time.NewTicker(time.Second)
	defer refresh.Stop()
	defer decay.Stop()

	// step.3 定时更新市场并按最近成交时间递减全局计数。
	for {
		select {
		case <-ctx.Done():
			restart(false)
			b.jobs.Wait()
			return nil
		case now := <-decay.C:
			b.mu.Lock()
			// 按完整间隔折算应扣减的买入计数。
			b.help_decay(now)
			b.mu.Unlock()
		case <-refresh.C:
			if err := b.refresh(ctx, false); err != nil {
				log.Printf("刷新市场失败: %v", err)
			}
			restart(true)
		}
	}
}

// refresh 同步可交易市场并维护现货成交额排行。
func (b *bot) refresh(ctx context.Context, initial bool) error {
	// step.1 获取有同名可交易现货的 USDT 合约。
	// 匹配合约与现货市场清单，确定行情订阅范围。
	rules, err := b.help_marketRules(ctx)
	if err != nil {
		return err
	}
	// step.2 更新交易规则并停止为已下架市场接收买入信号。
	b.mu.Lock()
	for symbol := range b.markets {
		if _, ok := rules[symbol]; !ok {
			delete(b.markets, symbol)
		}
	}
	for symbol, rule := range rules {
		if b.markets[symbol] == nil {
			b.markets[symbol] = &market{}
		}
		b.markets[symbol].rules = rule
	}
	// 市场状态变动后立即更新下单资格，避免沿用旧排行。
	b.help_rank()
	b.mu.Unlock()
	// step.3 补齐缺失的历史 K 线并更新成交额。
	// 首次加载全量现货成交额，之后每小时更新一项。
	b.help_refreshData(ctx, initial)
	return ctx.Err()
}

// onKline 更新单个市场的实时价格和布林带并分配全局买入次数。
func (b *bot) onKline(ctx context.Context, event *futures.WsKlineEvent) {
	// step.1 接收当前市场行情，遇到断档时先补齐历史。
	// 解析最新成交价并过滤无效数据。
	price := help_positive(event.Kline.Close)
	if price <= 0 || ctx.Err() != nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	m := b.markets[event.Symbol]
	if m == nil || event.Time < m.priceAt || event.Kline.EndTime <= m.closedAt {
		return
	}
	m.price, m.priceAt = event.Kline.Close, event.Time
	if m.closedAt != 0 && event.Kline.StartTime != m.closedAt+1 {
		// 补齐断线期间缺失的已收盘 K 线。
		b.help_scheduleBackfill(ctx, event.Symbol, m)
		return
	}

	// step.2 用已收盘 K 线更新下一周期的布林下轨。
	lower := m.lower
	if event.Kline.IsFinal {
		// 纳入本周期收盘价，供下一周期使用。
		m.closes, m.lower = help_boll(append(m.closes, price))
		m.closedAt = event.Kline.EndTime
	}

	// step.3 记录各订阅交易对相对布林下轨的最大偏离。
	if !m.loading {
		// 汇总本轮行情偏离，供五分钟状态日志使用。
		b.help_observeBoll(event.Symbol, price, lower)
	}

	// step.4 依全局次数和现货排行判断是否启动 GTX 买入。
	// 在分配次数前应用截至当前时间的递减。
	b.help_decay(time.Now())
	threshold := lower * (1 - b.cfg.InitialDrop - b.cfg.DropStep*float64(b.times))
	if lower <= 0 || !m.ranked || m.loading || b.busy[event.Symbol] || price >= threshold ||
		time.Since(time.UnixMilli(event.Time)) > 10*time.Second {
		return
	}
	times := b.times
	b.times++
	if times == 0 {
		b.decayAt = time.Now()
	}
	b.busy[event.Symbol] = true
	b.jobs.Add(1)
	go b.buy(b.ctx, event.Symbol, event.Kline.Close, times)
}
