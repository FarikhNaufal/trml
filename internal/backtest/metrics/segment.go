package metrics

import (
	"math"
	"sort"
	"time"
)

type SegmentStats struct {
	Name            string  `json:"name"`
	TotalTrades     int     `json:"total_trades"`
	Wins            int     `json:"wins"`
	Losses          int     `json:"losses"`
	BreakEven       int     `json:"break_even"`
	WinRatePct      float64 `json:"win_rate_pct"`
	TotalNetPnL     float64 `json:"total_net_pnl"`
	AvgPnLUSDT      float64 `json:"avg_pnl_usdt"`
	GrossProfit     float64 `json:"gross_profit"`
	GrossLoss       float64 `json:"gross_loss"`
	ProfitFactor    float64 `json:"profit_factor"`
	NetProfit       float64 `json:"net_profit"`
	NetLoss         float64 `json:"net_loss"`
	NetProfitFactor float64 `json:"net_profit_factor"`
}

// SymbolAttribution summarizes net profit/loss contribution by trading pair.
type SymbolAttribution struct {
	Symbol      string  `json:"symbol"`
	Trades      int     `json:"trades"`
	Wins        int     `json:"wins"`
	WinRatePct  float64 `json:"win_rate_pct"`
	TotalNetPnL float64 `json:"total_net_pnl"`
	TotalFees   float64 `json:"total_fees"`
}

type SegmentationReport struct {
	NATRSegments      []SegmentStats `json:"natr_segments"`
	SessionSegments   []SegmentStats `json:"session_segments"`
	SideSegments      []SegmentStats `json:"side_segments"`
	HoldingSegments   []SegmentStats `json:"holding_segments,omitempty"`
	EntryTypeSegments []SegmentStats `json:"entry_type_segments,omitempty"`
	StrategySegments  []SegmentStats `json:"strategy_segments,omitempty"`
}

// CalculateSegmentation mengelompokkan trade ke dalam bucket statistik bebas path-dependency.
func CalculateSegmentation(trades []AggregatedTrade) SegmentationReport {
	natrLow := newSegment("NATR < 0.80%")
	natrHigh := newSegment("NATR >= 0.80%")

	sessAsia := newSegment("Asia (00:00 - 08:00 UTC)")
	sessLondon := newSegment("London (08:00 - 14:00 UTC)")
	sessNY := newSegment("New York (14:00 - 21:00 UTC)")
	sessLate := newSegment("NY Close / Asia Early (21:00 - 24:00 UTC)")

	sideLong := newSegment("LONG Positions")
	sideShort := newSegment("SHORT Positions")

	holdQuick := newSegment("< 15m (Scalp)")
	holdShort := newSegment("15m - 1h")
	holdMed := newSegment("1h - 4h")
	holdLong := newSegment("> 4h")

	entryCleanLimit := newSegment("Clean Limit (No Repeg)")
	entryRepegged := newSegment("GTX Repeg")
	entryMarket := newSegment("Market Fill")

	byStrategy := make(map[string]*segmentAccumulator)

	for _, t := range trades {
		stratKey := t.StrategyID
		if stratKey == "" {
			stratKey = "VWAP_PULLBACK"
		}
		stratSeg, exists := byStrategy[stratKey]
		if !exists {
			stratSeg = newSegment(stratKey)
			byStrategy[stratKey] = stratSeg
		}
		stratSeg.addTrade(t)

		if t.NATRPct < 0.80 {
			natrLow.addTrade(t)
		} else {
			natrHigh.addTrade(t)
		}

		h := t.EntryTime.UTC().Hour()
		if h >= 0 && h < 8 {
			sessAsia.addTrade(t)
		} else if h >= 8 && h < 14 {
			sessLondon.addTrade(t)
		} else if h >= 14 && h < 21 {
			sessNY.addTrade(t)
		} else {
			sessLate.addTrade(t)
		}

		if t.Side == "SHORT" {
			sideShort.addTrade(t)
		} else {
			sideLong.addTrade(t)
		}

		dur := t.ExitTime.Sub(t.EntryTime)
		switch {
		case dur < 15*time.Minute:
			holdQuick.addTrade(t)
		case dur < time.Hour:
			holdShort.addTrade(t)
		case dur <= 4*time.Hour:
			holdMed.addTrade(t)
		default:
			holdLong.addTrade(t)
		}

		switch {
		case t.EntryMode == "MARKET":
			entryMarket.addTrade(t)
		case t.RepegCount > 0 || t.EntryMode == "GTX_REPEG":
			entryRepegged.addTrade(t)
		default:
			entryCleanLimit.addTrade(t)
		}
	}

	stratKeys := make([]string, 0, len(byStrategy))
	for k := range byStrategy {
		stratKeys = append(stratKeys, k)
	}
	sort.Strings(stratKeys)
	stratSegments := make([]SegmentStats, 0, len(stratKeys))
	for _, k := range stratKeys {
		stratSegments = append(stratSegments, byStrategy[k].finalize())
	}

	return SegmentationReport{
		NATRSegments: []SegmentStats{
			natrLow.finalize(),
			natrHigh.finalize(),
		},
		SessionSegments: []SegmentStats{
			sessAsia.finalize(),
			sessLondon.finalize(),
			sessNY.finalize(),
			sessLate.finalize(),
		},
		SideSegments: []SegmentStats{
			sideLong.finalize(),
			sideShort.finalize(),
		},
		HoldingSegments: []SegmentStats{
			holdQuick.finalize(),
			holdShort.finalize(),
			holdMed.finalize(),
			holdLong.finalize(),
		},
		EntryTypeSegments: []SegmentStats{
			entryCleanLimit.finalize(),
			entryRepegged.finalize(),
			entryMarket.finalize(),
		},
		StrategySegments: stratSegments,
	}
}

// CalculateSymbolAttribution returns the topN most profitable and bottomN worst symbols.
func CalculateSymbolAttribution(trades []AggregatedTrade, topN int) (top, bottom []SymbolAttribution) {
	if len(trades) == 0 || topN <= 0 {
		return nil, nil
	}
	bySym := make(map[string]*SymbolAttribution)
	for _, t := range trades {
		sa, ok := bySym[t.Symbol]
		if !ok {
			sa = &SymbolAttribution{Symbol: t.Symbol}
			bySym[t.Symbol] = sa
		}
		sa.Trades++
		if t.NetPnL > 0.0001 {
			sa.Wins++
		}
		sa.TotalNetPnL += t.NetPnL
		sa.TotalFees += t.TotalFees
	}

	all := make([]SymbolAttribution, 0, len(bySym))
	for _, sa := range bySym {
		if sa.Trades > 0 {
			sa.WinRatePct = (float64(sa.Wins) / float64(sa.Trades)) * 100.0
		}
		all = append(all, *sa)
	}

	// Best performers descending
	sort.Slice(all, func(i, j int) bool {
		if all[i].TotalNetPnL != all[j].TotalNetPnL {
			return all[i].TotalNetPnL > all[j].TotalNetPnL
		}
		return all[i].Symbol < all[j].Symbol
	})
	if len(all) <= topN {
		top = append([]SymbolAttribution(nil), all...)
	} else {
		top = append([]SymbolAttribution(nil), all[:topN]...)
	}

	// Worst performers ascending
	rev := make([]SymbolAttribution, len(all))
	copy(rev, all)
	sort.Slice(rev, func(i, j int) bool {
		if rev[i].TotalNetPnL != rev[j].TotalNetPnL {
			return rev[i].TotalNetPnL < rev[j].TotalNetPnL
		}
		return rev[i].Symbol < rev[j].Symbol
	})
	if len(rev) <= topN {
		bottom = rev
	} else {
		bottom = rev[:topN]
	}
	return top, bottom
}

type segmentAccumulator struct {
	stat SegmentStats
}

func newSegment(name string) *segmentAccumulator {
	return &segmentAccumulator{
		stat: SegmentStats{Name: name},
	}
}

func (s *segmentAccumulator) addTrade(t AggregatedTrade) {
	s.stat.TotalTrades++
	s.stat.TotalNetPnL += t.NetPnL
	if t.GrossPnL > 0 {
		s.stat.GrossProfit += t.GrossPnL
	} else {
		s.stat.GrossLoss += math.Abs(t.GrossPnL)
	}

	if t.NetPnL > 0.0001 {
		s.stat.Wins++
		s.stat.NetProfit += t.NetPnL
	} else if t.NetPnL < -0.0001 {
		s.stat.Losses++
		s.stat.NetLoss += math.Abs(t.NetPnL)
	} else {
		s.stat.BreakEven++
	}
}

func (s *segmentAccumulator) finalize() SegmentStats {
	if s.stat.TotalTrades > 0 {
		s.stat.WinRatePct = (float64(s.stat.Wins) / float64(s.stat.TotalTrades)) * 100.0
		s.stat.AvgPnLUSDT = s.stat.TotalNetPnL / float64(s.stat.TotalTrades)
	}
	if s.stat.GrossLoss > 0 {
		s.stat.ProfitFactor = s.stat.GrossProfit / s.stat.GrossLoss
	} else if s.stat.GrossProfit > 0 {
		s.stat.ProfitFactor = 99.0
	} else {
		s.stat.ProfitFactor = 0.0
	}
	if s.stat.NetLoss > 0 {
		s.stat.NetProfitFactor = s.stat.NetProfit / s.stat.NetLoss
	} else if s.stat.NetProfit > 0 {
		s.stat.NetProfitFactor = 99.0
	} else {
		s.stat.NetProfitFactor = 0.0
	}
	return s.stat
}

