import { Sparkles, Tag, BarChart3, FolderTree, ScanBarcode, Zap, type LucideIcon } from "lucide-react";
import { motion } from "framer-motion";

interface BenefitItem {
  icon: LucideIcon;
  title: string;
  desc: string;
  span: 1 | 2;
  accent: string;
  iconBg: string;
  iconColor: string;
}

const benefits: BenefitItem[] = [
  {
    icon: Sparkles,
    title: "Standardized names",
    desc: "Cryptic abbreviations become clean, readable product names.",
    span: 2,
    accent: "border-l-emerald-400/60",
    iconBg: "bg-emerald-50 dark:bg-emerald-950/30",
    iconColor: "text-emerald-600 dark:text-emerald-400",
  },
  {
    icon: Tag,
    title: "Brand & pack extraction",
    desc: "Buried metadata surfaced from any format.",
    span: 1,
    accent: "border-l-blue-400/60",
    iconBg: "bg-blue-50 dark:bg-blue-950/30",
    iconColor: "text-blue-600 dark:text-blue-400",
  },
  {
    icon: BarChart3,
    title: "Quality scored",
    desc: "Every row rated 0\u201310 so you know where to focus.",
    span: 1,
    accent: "border-l-amber-400/60",
    iconBg: "bg-amber-50 dark:bg-amber-950/30",
    iconColor: "text-amber-600 dark:text-amber-400",
  },
  {
    icon: FolderTree,
    title: "Category taxonomy",
    desc: "Consistent 3-level categorization across your entire catalog.",
    span: 2,
    accent: "border-l-violet-400/60",
    iconBg: "bg-violet-50 dark:bg-violet-950/30",
    iconColor: "text-violet-600 dark:text-violet-400",
  },
  {
    icon: ScanBarcode,
    title: "GTIN enrichment",
    desc: "Barcodes validated against global product databases.",
    span: 2,
    accent: "border-l-rose-400/60",
    iconBg: "bg-rose-50 dark:bg-rose-950/30",
    iconColor: "text-rose-600 dark:text-rose-400",
  },
  {
    icon: Zap,
    title: "Minutes, not weeks",
    desc: "Thousands of products in one run.",
    span: 1,
    accent: "border-l-cyan-400/60",
    iconBg: "bg-cyan-50 dark:bg-cyan-950/30",
    iconColor: "text-cyan-600 dark:text-cyan-400",
  },
];

const BenefitsSection = () => {
  return (
    <section className="py-20 lg:py-28 bg-background">
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-12"
        >
          <h2 className="font-heading text-3xl md:text-4xl font-bold text-foreground">
            What you get
          </h2>
          <p className="mt-3 text-muted-foreground text-lg max-w-md mx-auto">
            Your catalog, transformed in minutes
          </p>
        </motion.div>

        <div className="max-w-6xl mx-auto grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {benefits.map((b, i) => (
            <motion.div
              key={b.title}
              initial={{ opacity: 0, y: 16 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.06 }}
              className={`bg-card border border-border ${b.accent} border-l-[3px] rounded-lg px-4 py-3.5 hover:shadow-md transition-all duration-300 ${
                b.span === 2 ? "sm:col-span-2 lg:col-span-2" : ""
              }`}
            >
              <div className="flex items-start gap-3">
                <div className={`w-8 h-8 shrink-0 rounded-md ${b.iconBg} flex items-center justify-center`}>
                  <b.icon className={`w-4 h-4 ${b.iconColor}`} />
                </div>
                <div className="min-w-0">
                  <h3 className="font-heading text-sm font-semibold text-foreground">{b.title}</h3>
                  <p className="text-xs text-muted-foreground mt-0.5 leading-relaxed">{b.desc}</p>
                </div>
              </div>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
};

export default BenefitsSection;
