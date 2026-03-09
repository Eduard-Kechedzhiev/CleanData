import { Check } from "lucide-react";

interface StepperProps {
  currentStep: number; // 1-4
}

const steps = ["Upload", "Processing", "Results Preview", "Full Report"];

const Stepper = ({ currentStep }: StepperProps) => {
  return (
    <div className="flex items-center justify-center gap-1 py-4">
      {steps.map((label, i) => {
        const stepNum = i + 1;
        const isComplete = stepNum < currentStep;
        const isCurrent = stepNum === currentStep;

        return (
          <div key={label} className="flex items-center">
            <div className="flex flex-col items-center">
              <div
                className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold transition-colors ${
                  isComplete
                    ? "bg-primary text-primary-foreground"
                    : isCurrent
                    ? "bg-primary text-primary-foreground ring-4 ring-primary/20"
                    : "bg-muted text-muted-foreground"
                }`}
              >
                {isComplete ? <Check className="w-4 h-4" /> : stepNum}
              </div>
              <span
                className={`text-[10px] mt-1 font-medium hidden sm:block ${
                  isCurrent ? "text-primary" : "text-muted-foreground"
                }`}
              >
                {label}
              </span>
            </div>
            {i < steps.length - 1 && (
              <div
                className={`w-8 md:w-16 h-px mx-1 ${
                  isComplete ? "bg-primary" : "bg-border"
                }`}
              />
            )}
          </div>
        );
      })}
    </div>
  );
};

export default Stepper;
