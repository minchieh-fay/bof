package main

import (
	"context"
	"log"
	"time"
)

// buy 执行单个买入信号的 GTX 挂单、撤单与剩余数量重挂。
func (b *bot) buy(ctx context.Context, symbol, signalPrice string, times int) {
	// step.1 锁定本次信号对应的基础数量，并设置合约杠杆。
	defer b.jobs.Done()
	canUnlock := true
	defer func() {
		if canUnlock {
			b.mu.Lock()
			delete(b.busy, symbol)
			b.mu.Unlock()
		}
	}()
	// 计算本次买入计划的总名义金额和数量。
	remaining := help_buyQuantity(b.cfg, signalPrice, times)
	if _, err := b.futures.NewChangeLeverageService().Symbol(symbol).Leverage(b.cfg.Leverage).Do(ctx); err != nil {
		log.Printf("%s 设置杠杆失败: %v", symbol, err)
		return
	}

	// step.2 用实时价格提交 GTX 买单，等待后结算并依据成交量更新剩余数量。
	for ctx.Err() == nil && remaining.IsPositive() {
		// 检查市场是否还在排名内，并取得新鲜行情和交易规则。
		rule, price, ok := b.help_orderMarket(symbol)
		if !ok {
			return
		}
		// 在允许的价格档位提交一笔订单，并确认是否存在待结算订单。
		id, accepted, uncertain, retry := b.help_postBuy(ctx, rule, price, remaining)
		if retry {
			// 三档均冲突时等待下一轮，并沿用本次信号的额度。
			help_wait(ctx, 5*time.Second)
			continue
		}
		if !accepted && !uncertain {
			return
		}
		if accepted {
			// 为已受理的 GTX 订单保留五秒成交时间。
			help_wait(ctx, 5*time.Second)
		}
		// 撤销未成交部分，并以最终订单状态为准记录成交量。
		filled, settled := b.help_finishOrder(ctx, symbol, id)
		if filled.IsPositive() {
			b.mu.Lock()
			b.decayAt = time.Now()
			b.mu.Unlock()
			remaining = remaining.Sub(filled)
			log.Printf("%s 买入成交 %s，待买数量 %s，订单 %s", symbol, filled, remaining, id)
		}
		if !settled {
			canUnlock = false
			log.Printf("%s 订单 %s 状态待确认，暂停该交易对买入", symbol, id)
			return
		}
		if uncertain {
			return
		}
	}
}
