import { useState } from "react";
import { Upload, Cpu, MessageSquare } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  ReferenceLine,
} from "recharts";

/* ── How-it-works steps ── */

const steps = [
  {
    icon: Upload,
    num: "1",
    title: "Upload",
    desc: "Drop your catalog CSV — any format, any columns.",
  },
  {
    icon: Cpu,
    num: "2",
    title: "Process",
    desc: "AI cleans, scores, and enriches every product.",
  },
  {
    icon: MessageSquare,
    num: "3",
    title: "Get Results",
    desc: "Review the insights, then we walk you through the full report.",
  },
];

/* ── Graph metrics ── */

type MetricKey = "conversion" | "accuracy" | "discovery";

interface MetricConfig {
  key: MetricKey;
  label: string;
  unit: string;
  color: string;
  description: string;
  source: string;
  domain: [number, number];
  data: { score: number; value: number }[];
}

/**
 * Correlation data extrapolated from industry research onto the 0-10
 * quality score scale used by the CleanData pipeline.
 */
const metrics: MetricConfig[] = [
  {
    key: "conversion",
    label: "Conversion Rate",
    unit: "%",
    color: "#10b981",
    description:
      "Enriched product data drives up to 30% higher conversion rates for online distributors.",
    source: "Distributor Data Solutions & McKinsey",
    domain: [0, 5],
    data: [
      { score: 1, value: 1.2 },
      { score: 2, value: 1.6 },
      { score: 3, value: 2.1 },
      { score: 4, value: 2.5 },
      { score: 5, value: 2.9 },
      { score: 6, value: 3.3 },
      { score: 7, value: 3.6 },
      { score: 8, value: 3.9 },
      { score: 9, value: 4.1 },
      { score: 10, value: 4.2 },
    ],
  },
  {
    key: "accuracy",
    label: "Order Accuracy",
    unit: "%",
    color: "#3b82f6",
    description:
      "Accurate product descriptions reduce returns — 40% of consumers return products due to inaccurate content.",
    source: "Salsify Consumer Research & Feedonomics",
    domain: [75, 100],
    data: [
      { score: 1, value: 82.0 },
      { score: 2, value: 84.0 },
      { score: 3, value: 85.5 },
      { score: 4, value: 87.0 },
      { score: 5, value: 89.0 },
      { score: 6, value: 90.5 },
      { score: 7, value: 92.0 },
      { score: 8, value: 93.5 },
      { score: 9, value: 94.5 },
      { score: 10, value: 95.5 },
    ],
  },
  {
    key: "discovery",
    label: "Product Discovery",
    unit: "%",
    color: "#8b5cf6",
    description:
      "Clean product names make items findable in search — search users are 15% of visitors but drive 45% of revenue.",
    source: "McKinsey & Baymard Institute",
    domain: [0, 100],
    data: [
      { score: 1, value: 32 },
      { score: 2, value: 41 },
      { score: 3, value: 52 },
      { score: 4, value: 61 },
      { score: 5, value: 70 },
      { score: 6, value: 78 },
      { score: 7, value: 85 },
      { score: 8, value: 91 },
      { score: 9, value: 95 },
      { score: 10, value: 97 },
    ],
  },
];

const TYPICAL_SCORE = 5;
const CLEAN_SCORE = 9;

/* eslint-disable @typescript-eslint/no-explicit-any */
const CustomTooltip = ({ active, payload, label, metric }: any) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-card border border-border rounded-lg px-3 py-2 shadow-lg text-left">
      <p className="text-xs text-muted-foreground mb-1">
        Quality Score: {label}/10
      </p>
      <p className="text-sm font-semibold" style={{ color: metric.color }}>
        {metric.label}: {payload[0].value}
        {metric.unit}
      </p>
    </div>
  );
};

const BenchmarksSection = () => {
  const [active, setActive] = useState<MetricKey>("conversion");
  const current = metrics.find((m) => m.key === active)!;

  return (
    <section className="py-20 lg:py-28 bg-muted/30">
      <div className="container mx-auto px-4">
        {/* Two-column: steps left, graph right */}
        <div className="max-w-6xl mx-auto grid lg:grid-cols-[280px_1fr] gap-10 lg:gap-14 items-start">
          {/* Left — How it works (vertical) */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <h2 className="font-heading text-2xl md:text-3xl font-bold text-foreground mb-2">
              How it works
            </h2>
            <p className="text-muted-foreground text-sm mb-8">
              Three steps. Zero configuration.
            </p>

            <div className="relative flex flex-col gap-8">
              {/* Vertical connector */}
              <div className="absolute left-[23px] top-10 bottom-10 w-px bg-border" />

              {steps.map((step, i) => (
                <motion.div
                  key={step.title}
                  initial={{ opacity: 0, y: 16 }}
                  whileInView={{ opacity: 1, y: 0 }}
                  viewport={{ once: true }}
                  transition={{ delay: i * 0.12 }}
                  className="flex gap-4 relative"
                >
                  <div className="relative z-10 w-12 h-12 shrink-0 rounded-xl bg-card border-2 border-primary/20 flex items-center justify-center shadow-sm">
                    <step.icon className="w-5 h-5 text-primary" />
                  </div>
                  <div className="pt-1">
                    <span className="text-[10px] font-bold text-primary uppercase tracking-wider">
                      Step {step.num}
                    </span>
                    <h3 className="font-heading text-base font-semibold text-foreground leading-tight">
                      {step.title}
                    </h3>
                    <p className="text-sm text-muted-foreground mt-0.5">
                      {step.desc}
                    </p>
                  </div>
                </motion.div>
              ))}
            </div>
          </motion.div>

          {/* Right — Graph */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            whileInView={{ opacity: 1, x: 0 }}
            viewport={{ once: true }}
          >
            <h2 className="font-heading text-2xl md:text-3xl font-bold text-foreground mb-2">
              The data quality advantage
            </h2>
            <p className="text-muted-foreground text-sm mb-6">
              Higher quality scores correlate with measurable business outcomes
            </p>

            {/* Tab selector */}
            <div className="mb-5">
              <div className="inline-flex bg-card border border-border rounded-xl p-1 gap-1">
                {metrics.map((m) => (
                  <button
                    key={m.key}
                    onClick={() => setActive(m.key)}
                    className="relative px-3 py-1.5 text-sm font-medium rounded-lg transition-colors"
                  >
                    {active === m.key && (
                      <motion.div
                        layoutId="activePill"
                        className="absolute inset-0 rounded-lg"
                        style={{ backgroundColor: m.color, opacity: 0.12 }}
                        transition={{
                          type: "spring",
                          bounce: 0.2,
                          duration: 0.5,
                        }}
                      />
                    )}
                    <span
                      className="relative z-10 transition-colors"
                      style={{
                        color: active === m.key ? m.color : undefined,
                      }}
                    >
                      {m.label}
                    </span>
                  </button>
                ))}
              </div>
            </div>

            {/* Chart card */}
            <div className="bg-card border border-border rounded-2xl p-5 md:p-6">
              <AnimatePresence mode="wait">
                <motion.div
                  key={active}
                  initial={{ opacity: 0, y: 8 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -8 }}
                  transition={{ duration: 0.25 }}
                >
                  <ResponsiveContainer width="100%" height={280}>
                    <AreaChart
                      data={current.data}
                      margin={{ top: 12, right: 12, bottom: 8, left: 0 }}
                    >
                      <defs>
                        <linearGradient
                          id={`grad-${active}`}
                          x1="0"
                          y1="0"
                          x2="0"
                          y2="1"
                        >
                          <stop
                            offset="5%"
                            stopColor={current.color}
                            stopOpacity={0.25}
                          />
                          <stop
                            offset="95%"
                            stopColor={current.color}
                            stopOpacity={0}
                          />
                        </linearGradient>
                      </defs>

                      <CartesianGrid
                        strokeDasharray="3 3"
                        stroke="hsl(var(--border))"
                        strokeOpacity={0.5}
                      />

                      <XAxis
                        dataKey="score"
                        tick={{
                          fontSize: 12,
                          fill: "hsl(var(--muted-foreground))",
                        }}
                        axisLine={false}
                        tickLine={false}
                        label={{
                          value: "Quality Score",
                          position: "insideBottom",
                          offset: -4,
                          fontSize: 12,
                          fill: "hsl(var(--muted-foreground))",
                        }}
                      />

                      <YAxis
                        domain={current.domain}
                        tick={{
                          fontSize: 12,
                          fill: "hsl(var(--muted-foreground))",
                        }}
                        axisLine={false}
                        tickLine={false}
                        tickFormatter={(v: number) => `${v}${current.unit}`}
                        width={52}
                      />

                      <Tooltip
                        content={<CustomTooltip metric={current} />}
                        cursor={{
                          stroke: "hsl(var(--muted-foreground))",
                          strokeOpacity: 0.3,
                          strokeDasharray: "4 4",
                        }}
                      />

                      <ReferenceLine
                        x={TYPICAL_SCORE}
                        stroke="hsl(var(--muted-foreground))"
                        strokeDasharray="4 4"
                        strokeOpacity={0.6}
                      />
                      <ReferenceLine
                        x={CLEAN_SCORE}
                        stroke={current.color}
                        strokeDasharray="4 4"
                        strokeOpacity={0.7}
                      />

                      <Area
                        type="monotone"
                        dataKey="value"
                        stroke={current.color}
                        strokeWidth={2.5}
                        fillOpacity={1}
                        fill={`url(#grad-${active})`}
                        animationDuration={600}
                      />
                    </AreaChart>
                  </ResponsiveContainer>

                  {/* Legend */}
                  <div className="flex items-center justify-center gap-6 mt-2 text-xs text-muted-foreground">
                    <span className="flex items-center gap-1.5">
                      <span className="w-4 border-t-2 border-dashed border-muted-foreground/60" />
                      Most catalogs (score ~{TYPICAL_SCORE})
                    </span>
                    <span className="flex items-center gap-1.5">
                      <span
                        className="w-4 border-t-2 border-dashed"
                        style={{ borderColor: current.color }}
                      />
                      After CleanData (score ~{CLEAN_SCORE})
                    </span>
                  </div>
                </motion.div>
              </AnimatePresence>

              {/* Description + source */}
              <AnimatePresence mode="wait">
                <motion.div
                  key={`desc-${active}`}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.2 }}
                  className="mt-4 pt-4 border-t border-border"
                >
                  <p className="text-sm text-foreground/80">
                    {current.description}
                  </p>
                  <p className="text-[11px] text-muted-foreground/60 mt-1">
                    Source: {current.source}
                  </p>
                </motion.div>
              </AnimatePresence>
            </div>
          </motion.div>
        </div>
      </div>
    </section>
  );
};

export default BenchmarksSection;
