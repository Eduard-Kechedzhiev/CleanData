"""Gemini provider wrapper."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Type, TypeVar

from google import genai
from pydantic import BaseModel

from ..errors import ProviderError

logger = logging.getLogger(__name__)
SchemaT = TypeVar("SchemaT", bound=BaseModel)


class GeminiProvider:
    """Thin wrapper around Gemini model initialization and generation."""

    def __init__(self, model_name: str = "gemini-2.5-flash") -> None:
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ProviderError("GEMINI_API_KEY is required")
        timeout_raw = os.getenv("GEMINI_TIMEOUT_SECONDS", "").strip()
        timeout_seconds: Optional[float] = None
        if timeout_raw:
            try:
                timeout_seconds = float(timeout_raw)
            except Exception:
                timeout_seconds = None

        if timeout_seconds and timeout_seconds > 0:
            try:
                self.client = genai.Client(api_key=api_key, http_options={"timeout": timeout_seconds})
            except Exception:
                self.client = genai.Client(api_key=api_key)
        else:
            self.client = genai.Client(api_key=api_key)
        self.model_name = os.getenv("GEMINI_MODEL", model_name)

    def generate(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        config: Dict[str, Any] = {"temperature": 0}
        if system_prompt:
            config["system_instruction"] = system_prompt
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=config,
        )
        return (response.text or "").strip()

    async def generate_async(self, prompt: str, system_prompt: Optional[str] = None) -> str:
        config: Dict[str, Any] = {"temperature": 0}
        if system_prompt:
            config["system_instruction"] = system_prompt
        response = await self.client.aio.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config=config,
        )
        return (response.text or "").strip()

    @staticmethod
    def _extract_json_object(text: str) -> Dict[str, Any]:
        raw = text.strip()
        if raw.startswith("```"):
            lines = raw.splitlines()
            if len(lines) >= 3:
                raw = "\n".join(lines[1:-1]).strip()

        start = raw.find("{")
        end = raw.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ProviderError("AI response missing JSON object")
        return json.loads(raw[start : end + 1])

    def generate_with_schema(
        self,
        prompt: str,
        schema_model: Type[SchemaT],
        use_schema_generation: bool = True,
        system_prompt: Optional[str] = None,
    ) -> SchemaT:
        response = None
        generation_errors: List[str] = []

        if use_schema_generation:
            schema = schema_model.model_json_schema()
            config_candidates: List[Dict[str, Any]] = [
                {
                    "response_mime_type": "application/json",
                    "response_json_schema": schema,
                    "temperature": 0,
                },
                {
                    "response_mime_type": "application/json",
                    "response_schema": schema,
                    "temperature": 0,
                },
            ]
            if system_prompt:
                for config in config_candidates:
                    config["system_instruction"] = system_prompt

            for config in config_candidates:
                try:
                    response = self.client.models.generate_content(
                        model=self.model_name,
                        contents=prompt,
                        config=config,
                    )
                    break
                except Exception as exc:
                    generation_errors.append(str(exc))

        if response is None:
            if generation_errors:
                logger.warning(
                    "gemini_schema_generation_failed_falling_back",
                    extra={"errors": generation_errors},
                )
            try:
                fallback_config: Dict[str, Any] = {"temperature": 0}
                if system_prompt:
                    fallback_config["system_instruction"] = system_prompt
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=fallback_config,
                )
            except Exception as exc:
                if generation_errors:
                    raise ProviderError(
                        f"Gemini schema generation failed: {generation_errors}; fallback failed: {exc}"
                    ) from exc
                raise ProviderError(f"Gemini generation failed: {exc}") from exc

        return self._parse_schema_response(response, schema_model)

    async def generate_with_schema_async(
        self,
        prompt: str,
        schema_model: Type[SchemaT],
        use_schema_generation: bool = True,
        system_prompt: Optional[str] = None,
    ) -> SchemaT:
        """Async version using client.aio for better connection multiplexing."""
        response = None
        generation_errors: List[str] = []

        if use_schema_generation:
            schema = schema_model.model_json_schema()
            config_candidates: List[Dict[str, Any]] = [
                {
                    "response_mime_type": "application/json",
                    "response_json_schema": schema,
                    "temperature": 0,
                },
                {
                    "response_mime_type": "application/json",
                    "response_schema": schema,
                    "temperature": 0,
                },
            ]
            if system_prompt:
                for config in config_candidates:
                    config["system_instruction"] = system_prompt

            for config in config_candidates:
                try:
                    response = await self.client.aio.models.generate_content(
                        model=self.model_name,
                        contents=prompt,
                        config=config,
                    )
                    break
                except Exception as exc:
                    generation_errors.append(str(exc))

        if response is None:
            if generation_errors:
                logger.warning(
                    "gemini_schema_generation_failed_falling_back",
                    extra={"errors": generation_errors},
                )
            try:
                fallback_config: Dict[str, Any] = {"temperature": 0}
                if system_prompt:
                    fallback_config["system_instruction"] = system_prompt
                response = await self.client.aio.models.generate_content(
                    model=self.model_name,
                    contents=prompt,
                    config=fallback_config,
                )
            except Exception as exc:
                if generation_errors:
                    raise ProviderError(
                        f"Gemini schema generation failed: {generation_errors}; fallback failed: {exc}"
                    ) from exc
                raise ProviderError(f"Gemini generation failed: {exc}") from exc

        return self._parse_schema_response(response, schema_model)

    @staticmethod
    def _parse_schema_response(response: Any, schema_model: Type[SchemaT]) -> SchemaT:
        raw_text = (response.text or "").strip()
        try:
            parsed_payload = getattr(response, "parsed", None)
            if parsed_payload is not None:
                return schema_model.model_validate(parsed_payload)
            if not raw_text:
                raise ProviderError("Gemini returned empty response")
            payload = GeminiProvider._extract_json_object(raw_text)
            return schema_model.model_validate(payload)
        except Exception as exc:
            raise ProviderError(f"Failed to parse/validate schema response: {exc}") from exc
