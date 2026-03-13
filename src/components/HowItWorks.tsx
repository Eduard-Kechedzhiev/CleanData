import { Upload, Cpu, MessageSquare } from "lucide-react";
import { motion } from "framer-motion";

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

const HowItWorks = () => {
  return (
    <section className="py-20 lg:py-28 bg-muted/30">
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          className="text-center mb-14"
        >
          <h2 className="font-heading text-3xl md:text-4xl font-bold text-foreground">
            How it works
          </h2>
          <p className="mt-3 text-muted-foreground text-lg">
            Three steps. Zero configuration.
          </p>
        </motion.div>

        <div className="max-w-3xl mx-auto grid md:grid-cols-3 gap-8 relative">
          {/* Connector line */}
          <div className="hidden md:block absolute top-12 left-[16.67%] right-[16.67%] h-px bg-border" />

          {steps.map((step, i) => (
            <motion.div
              key={step.title}
              initial={{ opacity: 0, y: 20 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true }}
              transition={{ delay: i * 0.15 }}
              className="flex flex-col items-center text-center relative"
            >
              <div className="relative z-10 w-14 h-14 rounded-2xl bg-card border-2 border-primary/20 flex items-center justify-center mb-4 shadow-sm">
                <step.icon className="w-6 h-6 text-primary" />
              </div>
              <span className="text-xs font-bold text-primary mb-1">Step {step.num}</span>
              <h3 className="font-heading text-lg font-semibold text-foreground mb-1">{step.title}</h3>
              <p className="text-sm text-muted-foreground">{step.desc}</p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
};

export default HowItWorks;
