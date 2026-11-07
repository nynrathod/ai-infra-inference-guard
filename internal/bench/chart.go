package bench

import (
	"fmt"
	"io"
	"math"
	"strings"
)

// Bar is one bar in a chart: a label, a value and an optional second line
// under the label (e.g. the exact number).
type Bar struct {
	Label string
	Value float64
	Sub   string
}

// chart geometry, in SVG user units.
const (
	chartWidth   = 760
	chartHeight  = 420
	chartMarginL = 90
	chartMarginR = 30
	chartMarginT = 60
	chartMarginB = 90
)

// WriteBarChartSVG renders a simple labeled bar chart as standalone SVG.
// Charts are deterministic and dependency-free so benchmark reports can be
// committed to the repository and render anywhere (browsers, IDEs, GitHub).
func WriteBarChartSVG(w io.Writer, title, yLabel string, bars []Bar) error {
	if len(bars) == 0 {
		_, err := fmt.Fprintf(w, "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" height=\"%d\"><text x=\"20\" y=\"40\">%s: no data</text></svg>\n", chartWidth, chartHeight, escapeXML(title))
		return err
	}

	plotW := chartWidth - chartMarginL - chartMarginR
	plotH := chartHeight - chartMarginT - chartMarginB
	max := 0.0
	for _, b := range bars {
		if b.Value > max {
			max = b.Value
		}
	}
	if max <= 0 {
		max = 1
	}
	// Round the axis maximum up to a human number so gridlines are clean.
	max = niceCeil(max)

	slot := float64(plotW) / float64(len(bars))
	barW := slot * 0.6

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\" font-family=\"Helvetica,Arial,sans-serif\">\n", chartWidth, chartHeight, chartWidth, chartHeight))
	sb.WriteString("<rect width=\"100%\" height=\"100%\" fill=\"#ffffff\"/>\n")
	sb.WriteString(fmt.Sprintf("<text x=\"%d\" y=\"30\" font-size=\"17\" font-weight=\"bold\" fill=\"#1a1a2e\">%s</text>\n", chartMarginL, escapeXML(title)))

	// Gridlines and y labels at 0%, 25%, 50%, 75%, 100%.
	for i := 0; i <= 4; i++ {
		frac := float64(i) / 4
		y := float64(chartMarginT+plotH) - frac*float64(plotH)
		sb.WriteString(fmt.Sprintf("<line x1=\"%d\" y1=\"%.1f\" x2=\"%d\" y2=\"%.1f\" stroke=\"#e2e2ea\" stroke-width=\"1\"/>\n", chartMarginL, y, chartWidth-chartMarginR, y))
		sb.WriteString(fmt.Sprintf("<text x=\"%d\" y=\"%.1f\" font-size=\"11\" fill=\"#666\" text-anchor=\"end\">%s</text>\n", chartMarginL-8, y+4, formatAxis(max*frac)))
	}
	sb.WriteString(fmt.Sprintf("<text x=\"14\" y=\"%d\" font-size=\"12\" fill=\"#444\" transform=\"rotate(-90 14 %d)\" text-anchor=\"middle\">%s</text>\n", chartMarginT+plotH/2, chartMarginT+plotH/2, escapeXML(yLabel)))

	for i, b := range bars {
		x := chartMarginL + float64(i)*slot + (slot-barW)/2
		h := (b.Value / max) * float64(plotH)
		if h < 0 {
			h = 0
		}
		y := float64(chartMarginT+plotH) - h
		sb.WriteString(fmt.Sprintf("<rect x=\"%.1f\" y=\"%.1f\" width=\"%.1f\" height=\"%.1f\" rx=\"4\" fill=\"#4263eb\"/>\n", x, y, barW, h))
		// Value on top of the bar.
		sb.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%.1f\" font-size=\"12\" fill=\"#1a1a2e\" text-anchor=\"middle\" font-weight=\"bold\">%s</text>\n", x+barW/2, y-6, formatValue(b.Value)))
		// Label under the axis.
		labelY := chartMarginT + plotH + 20
		sb.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%d\" font-size=\"12\" fill=\"#333\" text-anchor=\"middle\">%s</text>\n", x+barW/2, labelY, escapeXML(b.Label)))
		if b.Sub != "" {
			sb.WriteString(fmt.Sprintf("<text x=\"%.1f\" y=\"%d\" font-size=\"11\" fill=\"#777\" text-anchor=\"middle\">%s</text>\n", x+barW/2, labelY+16, escapeXML(b.Sub)))
		}
	}

	sb.WriteString("</svg>\n")
	_, err := io.WriteString(w, sb.String())
	return err
}

// niceCeil rounds v up to 1/2/5 x 10^k so axis labels stay readable.
func niceCeil(v float64) float64 {
	if v <= 0 {
		return 1
	}
	exp := math.Floor(math.Log10(v))
	frac := v / math.Pow(10, exp)
	var nice float64
	switch {
	case frac <= 1:
		nice = 1
	case frac <= 2:
		nice = 2
	case frac <= 5:
		nice = 5
	default:
		nice = 10
	}
	return nice * math.Pow(10, exp)
}

func formatAxis(v float64) string {
	if v >= 100 || v == math.Trunc(v) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func formatValue(v float64) string {
	if v >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	if v >= 10 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.2f", v)
}

func escapeXML(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;", "'", "&apos;")
	return r.Replace(s)
}
