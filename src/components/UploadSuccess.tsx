import { CheckCircle, FileText } from "lucide-react";
import { motion } from "framer-motion";

interface UploadSuccessProps {
  queuePosition: number;
  fileName: string;
}

const UploadSuccess = ({ queuePosition, fileName }: UploadSuccessProps) => {
  return (
    <section className="min-h-[80vh] flex items-center justify-center bg-hero text-hero-foreground">
      <div className="container mx-auto px-4">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          className="max-w-lg mx-auto text-center"
        >
          <motion.div
            initial={{ scale: 0 }}
            animate={{ scale: 1 }}
            transition={{ delay: 0.2, type: "spring", bounce: 0.4 }}
            className="w-16 h-16 rounded-full bg-primary/20 flex items-center justify-center mx-auto mb-6"
          >
            <CheckCircle className="w-8 h-8 text-primary" />
          </motion.div>

          <h1 className="font-heading text-3xl md:text-4xl font-bold mb-3">
            Catalog received
          </h1>

          <div className="flex items-center justify-center gap-2 text-hero-muted mb-6">
            <FileText className="w-4 h-4" />
            <span className="text-sm">{fileName}</span>
          </div>

          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.4 }}
            className="bg-hero-foreground/5 border border-hero-muted/20 rounded-xl p-6 mb-6"
          >
            <p className="text-hero-muted text-sm mb-1">Your position in queue</p>
            <p className="font-heading text-5xl font-bold text-primary">
              #{queuePosition}
            </p>
          </motion.div>

          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.6 }}
            className="text-hero-muted text-base leading-relaxed"
          >
            Our team will review your catalog and reach out within{" "}
            <span className="text-hero-foreground font-medium">1–2 business days</span>{" "}
            with your results.
          </motion.p>
        </motion.div>
      </div>
    </section>
  );
};

export default UploadSuccess;
