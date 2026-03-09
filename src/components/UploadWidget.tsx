import { useState, useCallback } from "react";
import { Upload, FileText, Shield, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { motion, AnimatePresence } from "framer-motion";

interface UploadWidgetProps {
  onUpload: (file: File) => void;
  onSampleData: () => void;
  variant?: "hero" | "default";
}

const UploadWidget = ({ onUpload, onSampleData, variant = "hero" }: UploadWidgetProps) => {
  const [dragActive, setDragActive] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [error, setError] = useState<string | null>(null);

  const isHero = variant === "hero";

  const validateFile = (file: File): string | null => {
    const validTypes = [
      "text/csv",
      "text/tab-separated-values",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      "application/vnd.ms-excel",
    ];
    const validExts = [".csv", ".tsv", ".xlsx", ".xls"];
    const ext = "." + file.name.split(".").pop()?.toLowerCase();

    if (!validTypes.includes(file.type) && !validExts.includes(ext)) {
      return "Please upload a CSV, TSV, or Excel file";
    }
    if (file.size > 50 * 1024 * 1024) {
      return "File exceeds 50MB. For larger catalogs, contact us.";
    }
    if (file.size === 0) {
      return "This file appears to be empty";
    }
    return null;
  };

  const handleFile = useCallback((file: File) => {
    const err = validateFile(file);
    if (err) {
      setError(err);
      setSelectedFile(null);
    } else {
      setError(null);
      setSelectedFile(file);
    }
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragActive(false);
    if (e.dataTransfer.files[0]) handleFile(e.dataTransfer.files[0]);
  }, [handleFile]);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files?.[0]) handleFile(e.target.files[0]);
  };

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
    return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  };

  return (
    <div className="w-full max-w-lg mx-auto">
      <AnimatePresence mode="wait">
        {!selectedFile ? (
          <motion.div
            key="dropzone"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
          >
            <label
              className={`relative flex flex-col items-center justify-center rounded-xl border-2 border-dashed p-8 cursor-pointer transition-all duration-200 ${
                dragActive
                  ? "border-primary bg-primary/5 scale-[1.02]"
                  : isHero
                  ? "border-hero-muted/30 bg-hero-foreground/5 hover:border-primary/50 hover:bg-primary/5"
                  : "border-border bg-card hover:border-primary/50 hover:bg-accent"
              }`}
              onDragOver={(e) => { e.preventDefault(); setDragActive(true); }}
              onDragLeave={() => setDragActive(false)}
              onDrop={handleDrop}
            >
              <input
                type="file"
                accept=".csv,.tsv,.xlsx,.xls"
                className="hidden"
                onChange={handleInputChange}
              />
              <div className={`rounded-full p-3 mb-3 ${isHero ? "bg-primary/10" : "bg-accent"}`}>
                <Upload className="w-6 h-6 text-primary" />
              </div>
              <p className={`text-base font-medium mb-1 ${isHero ? "text-hero-foreground" : "text-foreground"}`}>
                Drop your catalog CSV here
              </p>
              <p className={`text-sm ${isHero ? "text-hero-muted" : "text-muted-foreground"}`}>
                or <span className="text-primary underline">browse files</span>
              </p>
              <p className={`text-xs mt-2 ${isHero ? "text-hero-muted/60" : "text-muted-foreground/60"}`}>
                CSV, TSV, or Excel • Up to 50MB
              </p>
            </label>

            {error && (
              <motion.p
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                className="text-destructive text-sm mt-2 text-center"
              >
                {error}
              </motion.p>
            )}

            <button
              onClick={onSampleData}
              className={`block mx-auto mt-4 text-sm underline underline-offset-4 transition-colors ${
                isHero ? "text-hero-muted hover:text-primary" : "text-muted-foreground hover:text-primary"
              }`}
            >
              Try with sample data
            </button>
          </motion.div>
        ) : (
          <motion.div
            key="preview"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className={`rounded-xl border p-6 ${
              isHero ? "border-hero-muted/20 bg-hero-foreground/5" : "border-border bg-card"
            }`}
          >
            <div className="flex items-start gap-3">
              <div className="rounded-lg bg-primary/10 p-2">
                <FileText className="w-5 h-5 text-primary" />
              </div>
              <div className="flex-1 min-w-0">
                <p className={`font-medium truncate ${isHero ? "text-hero-foreground" : "text-foreground"}`}>
                  {selectedFile.name}
                </p>
                <p className={`text-sm ${isHero ? "text-hero-muted" : "text-muted-foreground"}`}>
                  {formatSize(selectedFile.size)}
                </p>
              </div>
              <button
                onClick={() => { setSelectedFile(null); setError(null); }}
                className={`p-1 rounded-md transition-colors ${
                  isHero ? "text-hero-muted hover:text-hero-foreground" : "text-muted-foreground hover:text-foreground"
                }`}
              >
                <X className="w-4 h-4" />
              </button>
            </div>

            <Button
              variant={isHero ? "cta-dark" : "cta"}
              size="lg"
              className="w-full mt-4"
              onClick={() => onUpload(selectedFile)}
            >
              Upload & Start
            </Button>

            <button
              onClick={() => { setSelectedFile(null); setError(null); }}
              className={`block mx-auto mt-3 text-sm transition-colors ${
                isHero ? "text-hero-muted hover:text-primary" : "text-muted-foreground hover:text-primary"
              }`}
            >
              Choose different file
            </button>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Security message */}
      <div className={`flex items-center justify-center gap-1.5 mt-4 ${
        isHero ? "text-hero-muted/60" : "text-muted-foreground/60"
      }`}>
        <Shield className="w-3.5 h-3.5" />
        <span className="text-xs">Encrypted in transit & at rest. Auto-deleted within 24 hours.</span>
      </div>
    </div>
  );
};

export default UploadWidget;
