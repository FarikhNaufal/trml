package metrics

import (
	"math"
)

type SegmentStats struct {
	Name         string  `json:"name"`
	TotalTrades  int     `json:"total_trades"`
	Wins         int     `json:"wins"`
	Losses       int     `json:"losses"`
	BreakEven    int     `json:"break_even"`
	WinRatePct   float64 `json:"win_rate_pct"`
	TotalNetPnL  float64 `json:"total_net_pnl"`
	AvgPnLUSDT   float64 `json:"avg_pnl_usdt"`
	GrossProfit  float64 `json:"gross_profit"`
	GrossLoss    float64 `json:"gross_loss"`
	ProfitFactor float64 `json:"profit_factor"`
}

type SegmentationReport struct {
	NATRSegments    []SegmentStats `json:"natr_segments"`
	SessionSegments []SegmentStats `json:"session_segments"`
	SideSegments    []SegmentStats `json:"side_segments"`
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

	for _, t := range trades {
		// 1. NATR
		if t.NATRPct < 0.80 {
			natrLow.addTrade(t)
		} else {
			natrHigh.addTrade(t)
		}

		// 2. Session
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

		// 3. Side
		if t.Side == "SHORT" {
			sideShort.addTrade(t)
		} else {
			sideLong.addTrade(t)
		}
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
	}
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
	} else if t.NetPnL < -0.0001 {
		s.stat.Losses++
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
	return s.stat
}

