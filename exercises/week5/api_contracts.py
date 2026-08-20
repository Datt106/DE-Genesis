from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Callable

import requests


SOAP_ENV_NS = "http://schemas.xmlsoap.org/soap/envelope/"
PROMOTION_NS = "urn:de-genesis:promotions:v1"


@dataclass(frozen=True)
class RestAuth:
    """Contract xác thực REST độc lập với implementation của nhà cung cấp."""

    mode: str = "none"
    api_key: str | None = None
    access_token: str | None = None
    api_key_header: str = "X-API-Key"

    def headers(self) -> dict[str, str]:
        if self.mode == "none":
            return {}
        if self.mode == "api_key":
            if not self.api_key:
                raise ValueError("Thiếu API key")
            return {self.api_key_header: self.api_key}
        if self.mode == "oauth2":
            if not self.access_token:
                raise ValueError("Thiếu OAuth2 access token")
            return {"Authorization": f"Bearer {self.access_token}"}
        raise ValueError(f"Kiểu xác thực REST không hỗ trợ: {self.mode}")


def request_client_credentials_token(
    token_url: str,
    *,
    client_id: str,
    client_secret: str,
    scope: str | None = None,
    session: requests.Session | None = None,
    timeout: float = 5,
) -> str:
    """Đổi client credential lấy token; secret chỉ đi qua runtime, không ghi file."""

    client = session or requests.Session()
    form = {"grant_type": "client_credentials"}
    if scope:
        form["scope"] = scope
    response = client.post(
        token_url,
        data=form,
        auth=(client_id, client_secret),
        timeout=timeout,
    )
    response.raise_for_status()
    body = response.json()
    token = body.get("access_token") if isinstance(body, dict) else None
    if not token:
        raise ValueError("OAuth2 token response thiếu access_token")
    return str(token)


def build_soap_envelope(operation: str, fields: dict[str, Any]) -> bytes:
    """Tạo SOAP 1.1 envelope tối giản để adapter có thể được test offline."""

    envelope = ET.Element(ET.QName(SOAP_ENV_NS, "Envelope"))
    body = ET.SubElement(envelope, ET.QName(SOAP_ENV_NS, "Body"))
    request = ET.SubElement(body, ET.QName(PROMOTION_NS, operation))
    for name, value in fields.items():
        child = ET.SubElement(request, ET.QName(PROMOTION_NS, name))
        child.text = str(value)
    return ET.tostring(envelope, encoding="utf-8", xml_declaration=True)


def parse_soap_promotions(xml_payload: str | bytes) -> list[dict[str, Any]]:
    """Chuẩn hóa SOAP response về cùng contract dict với REST Promotion API."""

    root = ET.fromstring(xml_payload)
    fault = root.find(f".//{{{SOAP_ENV_NS}}}Fault")
    if fault is not None:
        message = fault.findtext("faultstring") or "SOAP Fault"
        raise ValueError(message)
    promotions: list[dict[str, Any]] = []
    for item in root.findall(f".//{{{PROMOTION_NS}}}promotion"):
        record = {
            child.tag.rsplit("}", 1)[-1]: child.text
            for child in list(item)
        }
        if record:
            promotions.append(record)
    return promotions


def fetch_soap_promotions(
    endpoint: str,
    *,
    operation: str = "ListPromotions",
    fields: dict[str, Any] | None = None,
    transport: Callable[..., Any] | None = None,
    timeout: float = 5,
) -> list[dict[str, Any]]:
    """Adapter SOAP có thể inject transport giả trong unit test."""

    request = transport or requests.post
    response = request(
        endpoint,
        data=build_soap_envelope(operation, fields or {}),
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": operation,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return parse_soap_promotions(response.content)
