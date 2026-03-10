import { useEffect, useState } from "react";
import { getPublicConfig } from "@/lib/api";

const DEFAULT_CONTACT_EMAIL = "hello@cleandata.com";

export function useContactEmail() {
  const [contactEmail, setContactEmail] = useState(DEFAULT_CONTACT_EMAIL);

  useEffect(() => {
    let cancelled = false;

    getPublicConfig()
      .then((config) => {
        if (!cancelled && config.contact_email) {
          setContactEmail(config.contact_email);
        }
      })
      .catch(() => {
        // Keep the default email if config cannot be loaded.
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return contactEmail;
}
