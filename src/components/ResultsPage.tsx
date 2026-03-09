import { useState } from "react";
import { Download, Lock, BarChart3, MessageSquare, Eye } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import Stepper from "./Stepper";
import { motion } from "framer-motion";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from "recharts";

interface ResultsPageProps {
  capturedEmail: string | null;
  onEmailCapture: (email: string) => void;
}

const summaryStats = [
  { label: "Products processed", value: "8,863", sub: null },
  { label: "Avg quality score", value: "8.4", sub: "/ 10", highlight: true },
  { label: "Brands extracted", value: "347", sub: "unique" },
  { label: "GTINs found", value: "72%", sub: "validated" },
];

const beforeAfterRows = [
  { original: "PORK LOINS BNLS CNTR CUT REF", cleaned: "Pork Loins Boneless Center Cut Refrigerated", brand: "Hatfield", pack: "5 x 8 LB", category: "Meats > Pork > Loins", score: 9 },
  { original: "CHKN BRST BNLS SKNLS FRZ 4OZ", cleaned: "Chicken Breast Boneless Skinless Frozen 4oz", brand: "Tyson", pack: "2 x 10 LB", category: "Meats > Poultry > Breast", score: 8 },
  { original: "PPR TOWEL 2PLY CS/30", cleaned: "Paper Towel 2-Ply Case of 30", brand: "Bounty", pack: "1 x 30 CT", category: "Janitorial > Paper > Towels", score: 7 },
  { original: "MAYO PKT .44OZ 200CT", cleaned: "Mayonnaise Packet 0.44oz 200 Count", brand: "Heinz", pack: "1 x 200 CT", category: "Condiments > Mayo > Packets", score: 9 },
  { original: "FRZ FRIES CRINKLE 5LB", cleaned: "Frozen French Fries Crinkle Cut 5lb", brand: "McCain", pack: "6 x 5 LB", category: "Frozen > Potatoes > Fries", score: 8 },
];

const scoreDistribution = [
  { score: "1-2", count: 12, color: "hsl(var(--score-poor))" },
  { score: "3-4", count: 45, color: "hsl(var(--score-poor))" },
  { score: "5-6", count: 198, color: "hsl(var(--score-good))" },
  { score: "7-8", count: 3842, color: "hsl(var(--score-excellent))" },
  { score: "9-10", count: 4766, color: "hsl(var(--score-excellent))" },
];

const topBrands = [
  { name: "Tyson", count: 423 },
  { name: "Sysco", count: 387 },
  { name: "Hatfield", count: 312 },
  { name: "Heinz", count: 289 },
  { name: "Bounty", count: 201 },
  { name: "McCain", count: 178 },
  { name: "Kellogg's", count: 156 },
  { name: "Kraft", count: 142 },
];

const ResultsPage = ({ capturedEmail, onEmailCapture }: ResultsPageProps) => {
  const [email, setEmail] = useState("");
  const [company, setCompany] = useState("");
  const [unlocked, setUnlocked] = useState(!!capturedEmail);
  const [showGate, setShowGate] = useState(false);

  const handleGateSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (email.includes("@")) {
      onEmailCapture(email);
      setUnlocked(true);
      setShowGate(false);
    }
  };

  const scoreColor = (s: number) =>
    s >= 8 ? "text-score-excellent" : s >= 5 ? "text-score-good" : "text-score-poor";

  return (
    <div className="min-h-screen bg-background">
      <div className="container mx-auto px-4 py-6 flex items-center justify-between">
        <a href="/" className="font-heading text-xl font-bold text-foreground">
          <span className="text-primary">Clean</span>Data
        </a>
        <Button variant="default" size="sm">
          <MessageSquare className="w-4 h-4" />
          Talk to Our Team
        </Button>
      </div>

      <div className="container mx-auto px-4 py-2">
        <Stepper currentStep={unlocked ? 4 : 3} />
      </div>

      <div className="container mx-auto px-4 py-8 max-w-6xl">
        {/* Summary Hero Card */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-10"
        >
          {summaryStats.map((stat) => (
            <div
              key={stat.label}
              className={`bg-card border rounded-xl p-6 text-center ${
                stat.highlight ? "border-primary/30 shadow-md" : "border-border"
              }`}
            >
              <p className={`font-heading text-3xl md:text-4xl font-bold ${
                stat.highlight ? "text-score-excellent" : "text-foreground"
              }`}>
                {stat.value}
                {stat.sub && (
                  <span className="text-base font-normal text-muted-foreground ml-1">{stat.sub}</span>
                )}
              </p>
              <p className="text-sm text-muted-foreground mt-1">{stat.label}</p>
            </div>
          ))}
        </motion.div>

        {/* Before/After Preview */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.1 }}
          className="mb-10"
        >
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-heading text-xl font-semibold text-foreground">
              Before → After Preview
            </h2>
            <button
              onClick={() => unlocked ? null : setShowGate(true)}
              className="text-sm text-primary font-medium flex items-center gap-1"
            >
              {unlocked ? <Eye className="w-4 h-4" /> : <Lock className="w-4 h-4" />}
              {unlocked ? "View all rows" : "See all rows"}
            </button>
          </div>
          <div className="overflow-x-auto rounded-xl border border-border bg-card">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Original</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Cleaned Name</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Brand</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Pack/Size</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Category</th>
                  <th className="text-center px-4 py-3 font-medium text-muted-foreground">Score</th>
                </tr>
              </thead>
              <tbody>
                {beforeAfterRows.map((row, i) => (
                  <tr key={i} className="border-b border-border/50 last:border-0">
                    <td className="px-4 py-3 text-muted-foreground font-mono text-xs max-w-[180px] truncate">{row.original}</td>
                    <td className="px-4 py-3 text-foreground font-medium">{row.cleaned}</td>
                    <td className="px-4 py-3 text-foreground">{row.brand}</td>
                    <td className="px-4 py-3 text-foreground">{row.pack}</td>
                    <td className="px-4 py-3 text-foreground text-xs">{row.category}</td>
                    <td className={`px-4 py-3 text-center font-bold ${scoreColor(row.score)}`}>{row.score}/10</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </motion.div>

        {/* Charts */}
        <div className="grid md:grid-cols-2 gap-6 mb-10">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.2 }}
            className="bg-card border border-border rounded-xl p-6"
          >
            <h3 className="font-heading font-semibold text-foreground mb-4">Quality Score Distribution</h3>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={scoreDistribution}>
                <XAxis dataKey="score" tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <Tooltip
                  contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: 8, fontSize: 13 }}
                />
                <Bar dataKey="count" radius={[6, 6, 0, 0]}>
                  {scoreDistribution.map((entry, i) => (
                    <Cell key={i} fill={entry.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.3 }}
            className="bg-card border border-border rounded-xl p-6"
          >
            <h3 className="font-heading font-semibold text-foreground mb-4">Top Brands Extracted</h3>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={topBrands} layout="vertical">
                <XAxis type="number" tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <YAxis type="category" dataKey="name" tick={{ fontSize: 12, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} width={70} />
                <Tooltip
                  contentStyle={{ background: "hsl(var(--card))", border: "1px solid hsl(var(--border))", borderRadius: 8, fontSize: 13 }}
                />
                <Bar dataKey="count" fill="hsl(var(--primary))" radius={[0, 6, 6, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </motion.div>
        </div>

        {/* Pepper CTA */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.35 }}
          className="bg-hero rounded-xl p-8 text-center mb-10"
        >
          <p className="text-hero-foreground font-heading text-xl font-semibold mb-2">
            Your catalog has 8,863 products ready for image sourcing.
          </p>
          <p className="text-hero-muted mb-5">Want us to help?</p>
          <Button variant="cta-dark" size="lg">
            <MessageSquare className="w-5 h-5" />
            Talk to Our Team
          </Button>
        </motion.div>

        {/* Download / Gate */}
        {unlocked ? (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-accent border border-primary/20 rounded-xl p-8 text-center"
          >
            <Download className="w-10 h-10 text-primary mx-auto mb-3" />
            <h3 className="font-heading text-xl font-semibold text-foreground mb-2">Your full report is ready</h3>
            <p className="text-sm text-muted-foreground mb-5">
              Includes cleaned names, brands, pack sizes, categories, quality scores, and GTIN status for all 8,863 products.
            </p>
            <div className="flex flex-wrap gap-3 justify-center">
              <Button variant="cta" size="lg">
                <Download className="w-5 h-5" />
                Download Full CSV
              </Button>
              <Button variant="outline" size="lg">
                Brand Summary Sheet
              </Button>
              <Button variant="outline" size="lg">
                Quality Breakdown
              </Button>
            </div>
            <p className="text-xs text-muted-foreground mt-4">
              ⏱ Results expire in 24 hours
            </p>
          </motion.div>
        ) : (
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            className="bg-card border border-border rounded-xl p-8 text-center relative overflow-hidden"
          >
            <div className="absolute inset-0 bg-gradient-to-t from-card via-card/80 to-transparent pointer-events-none" />
            <div className="relative z-10">
              <Lock className="w-10 h-10 text-muted-foreground mx-auto mb-3" />
              <h3 className="font-heading text-xl font-semibold text-foreground mb-2">
                Enter your email to download your full report
              </h3>
              <p className="text-sm text-muted-foreground mb-6 max-w-md mx-auto">
                Get the complete enriched CSV with cleaned names, brands, categories, quality scores, and GTIN validation for all 8,863 products.
              </p>
              <form onSubmit={handleGateSubmit} className="flex flex-col sm:flex-row gap-3 max-w-md mx-auto">
                <Input
                  type="email"
                  placeholder="you@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                />
                <Input
                  type="text"
                  placeholder="Company name"
                  value={company}
                  onChange={(e) => setCompany(e.target.value)}
                />
                <Button type="submit" variant="cta" size="default" className="whitespace-nowrap">
                  Download Report
                </Button>
              </form>
            </div>
          </motion.div>
        )}
      </div>
    </div>
  );
};

export default ResultsPage;
