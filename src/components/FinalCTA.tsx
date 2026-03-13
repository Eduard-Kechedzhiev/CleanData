import { Button } from "@/components/ui/button";
import { ArrowUp } from "lucide-react";
import { motion } from "framer-motion";

const FinalCTA = () => {
  const scrollToTop = () => window.scrollTo({ top: 0, behavior: "smooth" });

  return (
    <section className="py-20 lg:py-28 bg-hero relative overflow-hidden">
      <div className="absolute inset-0 opacity-[0.03]" style={{
        backgroundImage: "linear-gradient(hsl(var(--hero-foreground)) 1px, transparent 1px), linear-gradient(90deg, hsl(var(--hero-foreground)) 1px, transparent 1px)",
        backgroundSize: "60px 60px",
      }} />
      <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[500px] h-[500px] rounded-full bg-primary/8 blur-[120px] pointer-events-none" />

      <div className="relative container mx-auto px-4 text-center">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
        >
          <h2 className="font-heading text-3xl md:text-4xl font-bold text-hero-foreground mb-4 text-balance">
            Ready to see what's in your catalog?
          </h2>
          <p className="text-hero-muted text-lg mb-8 max-w-md mx-auto">
            Upload your CSV and get a full catalog analysis in minutes. Completely free.
          </p>
          <Button
            variant="cta-dark"
            size="xl"
            onClick={scrollToTop}
          >
            <ArrowUp className="w-5 h-5" />
            Upload your catalog
          </Button>
        </motion.div>
      </div>
    </section>
  );
};

export default FinalCTA;
