import { useState, useCallback } from "react";
import Navbar from "@/components/Navbar";
import HeroSection from "@/components/HeroSection";
import BenefitsSection from "@/components/BenefitsSection";
import SampleResults from "@/components/SampleResults";
import BenchmarksSection from "@/components/BenchmarksSection";
import FAQSection from "@/components/FAQSection";
import FinalCTA from "@/components/FinalCTA";
import UploadSuccess from "@/components/UploadSuccess";
import { uploadFile } from "@/lib/api";
import { useToast } from "@/hooks/use-toast";

const Index = () => {
  const { toast } = useToast();
  const [uploading, setUploading] = useState(false);
  const [uploadResult, setUploadResult] = useState<{
    queuePosition: number;
    fileName: string;
  } | null>(null);

  const handleUpload = useCallback(async (file: File) => {
    setUploading(true);
    try {
      const res = await uploadFile(file);
      setUploadResult({
        queuePosition: res.queue_position,
        fileName: file.name,
      });
    } catch (err: any) {
      toast({
        title: "Upload failed",
        description: err.message || "Something went wrong. Please try again.",
        variant: "destructive",
      });
      setUploading(false);
    }
  }, [toast]);

  const handleSampleData = useCallback(() => {
    const el = document.getElementById("sample-results");
    if (el) el.scrollIntoView({ behavior: "smooth" });
  }, []);

  if (uploadResult) {
    return (
      <div className="min-h-screen">
        <Navbar />
        <UploadSuccess
          queuePosition={uploadResult.queuePosition}
          fileName={uploadResult.fileName}
        />
      </div>
    );
  }

  return (
    <div className="min-h-screen">
      <Navbar />
      <HeroSection onUpload={handleUpload} onSampleData={handleSampleData} uploading={uploading} />
      <BenefitsSection />
      <SampleResults onTrySample={() => window.scrollTo({ top: 0, behavior: "smooth" })} />
      <BenchmarksSection />
      <FAQSection />
      <FinalCTA />
    </div>
  );
};

export default Index;
