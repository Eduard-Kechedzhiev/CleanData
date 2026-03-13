import UploadWidget from "./UploadWidget";
import { motion } from "framer-motion";

interface HeroSectionProps {
  onUpload: (file: File) => void;
  onSampleData: () => void;
  uploading?: boolean;
}

const HeroSection = ({ onUpload, onSampleData, uploading }: HeroSectionProps) => {
  return (
    <section className="relative bg-hero min-h-[90vh] flex items-center overflow-hidden">
      {/* Subtle grid background */}
      <div className="absolute inset-0 opacity-[0.03]" style={{
        backgroundImage: "linear-gradient(hsl(var(--hero-foreground)) 1px, transparent 1px), linear-gradient(90deg, hsl(var(--hero-foreground)) 1px, transparent 1px)",
        backgroundSize: "60px 60px",
      }} />

      {/* Gradient orb */}
      <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] rounded-full bg-primary/8 blur-[120px] pointer-events-none" />

      <div className="relative container mx-auto px-4 py-20 lg:py-28">
        <div className="max-w-2xl mx-auto text-center">
          <motion.h1
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6 }}
            className="font-heading text-4xl md:text-5xl lg:text-6xl font-bold text-hero-foreground leading-tight text-balance"
          >
            See what's hiding in your{" "}
            <span className="text-primary">product catalog</span>
          </motion.h1>

          <motion.p
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.15 }}
            className="mt-5 text-lg md:text-xl text-hero-muted max-w-xl mx-auto text-balance"
          >
            Upload your CSV and get a full catalog analysis with quality scores, brand extraction, and category taxonomy — free.
          </motion.p>

          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, delay: 0.3 }}
            className="mt-10"
          >
            <UploadWidget onUpload={onUpload} onSampleData={onSampleData} variant="hero" uploading={uploading} />
          </motion.div>
        </div>
      </div>
    </section>
  );
};

export default HeroSection;
