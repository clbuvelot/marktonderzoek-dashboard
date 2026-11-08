// Azure AI Foundry — Claude via de Anthropic-compatibele endpoint
const AZURE_ENDPOINT = "https://nl-ai-fndry.services.ai.azure.com/anthropic/v1/messages";
const AZURE_MODEL = "claude-sonnet-5-5";

// Versleutelde API key — gegenereerd door versleutel_key.py
// !! Vervang deze waarde met de output van: python versleutel_key.py !!
const ENCRYPTED_KEY = "5A8y7NN01RtcHMNDrcZOXz/twsHmd5qkVj4SO+PykN/iJzlYNH+ya7oyt/1eiabR7Qna4N4s13HissYN7g19VZmBRydhj3KaPRhl62h+WPZHUrmMElDmHkvnHEC4qIZA8EGxxioSkNTp5LtAGSs5NOwg+hvaX5cdDi9rzB6azZk=";

let AZURE_API_KEY = "";
try { AZURE_API_KEY = sessionStorage.getItem("azure_key") || ""; } catch (_) {}

function toonLogin() {
  document.getElementById("login-overlay").classList.add("open");
  document.getElementById("workspace").inert = true;
  document.getElementById("login-key").focus();
}

async function ontsleutelKey(wachtwoord) {
  try {
    const raw = Uint8Array.from(atob(ENCRYPTED_KEY), c => c.charCodeAt(0));
    const salt = raw.slice(0, 16);
    const iv = raw.slice(16, 28);
    const tag = raw.slice(28, 44);
    const ciphertext = raw.slice(44);
    // Combineer ciphertext + tag (Web Crypto verwacht dit achter elkaar)
    const combined = new Uint8Array(ciphertext.length + tag.length);
    combined.set(ciphertext);
    combined.set(tag, ciphertext.length);
    // PBKDF2 key derivation
    const keyMaterial = await crypto.subtle.importKey("raw", new TextEncoder().encode(wachtwoord), "PBKDF2", false, ["deriveKey"]);
    const aesKey = await crypto.subtle.deriveKey(
      { name: "PBKDF2", salt, iterations: 100000, hash: "SHA-256" },
      keyMaterial,
      { name: "AES-GCM", length: 256 },
      false,
      ["decrypt"]
    );
    const decrypted = await crypto.subtle.decrypt({ name: "AES-GCM", iv }, aesKey, combined);
    return new TextDecoder().decode(decrypted);
  } catch(e) {
    return null;
  }
}

async function submitLogin() {
  const wachtwoord = document.getElementById("login-key").value.trim();
  if (!wachtwoord) {
    document.getElementById("login-error").textContent = "Voer een wachtwoord in";
    return;
  }
  document.getElementById("login-error").textContent = "Ontsleutelen...";
  // AES-GCM verifieert zelf de auth-tag: bij een fout wachtwoord faalt het
  // ontsleutelen en komt er null terug.
  const key = await ontsleutelKey(wachtwoord);
  if (!key) {
    document.getElementById("login-error").textContent = "Onjuist wachtwoord";
    return;
  }
  AZURE_API_KEY = key;
  try { sessionStorage.setItem("azure_key", key); } catch (_) {}
  document.getElementById("login-overlay").classList.remove("open");
  document.getElementById("workspace").inert = false;
  document.getElementById("login-error").textContent = "";
  loadData();
}

// Toon login bij pagina laden als er geen key is
if (!AZURE_API_KEY) {
  window.addEventListener("DOMContentLoaded", toonLogin);
} else {
  loadData();
}

let chatDocTekst = "";
let chatRecordMeta = "";
let chatGeschiedenis = [];

async function openDocChat(pubId, titel) {
  chatGeschiedenis = [];
  chatDocTekst = "";
  chatRecordMeta = "";

  // Record metadata ophalen uit DATA
  const record = DATA.find(r => r.id === pubId);
  if (record) {
    const c = record.classificatie || {};
    chatRecordMeta = `
METADATA UIT AANBESTEDINGSREGISTER:
- Titel: ${record.titel || "—"}
- Opdrachtgever: ${record.aanbestedende_dienst || "—"}
- Publicatiedatum: ${record.publicatiedatum || "—"}
- Sluitingsdatum inschrijving: ${record.sluitingsdatum || "—"}
- Gunningsdatum: ${record.gunningsdatum || "—"}
- Status: ${record.publicatie_type || "—"}
- Procedure: ${record.procedure_type || "—"}
- Aard opdracht: ${record.type_opdracht_omschrijving || "—"}
- Europees/Nationaal: ${record.europees ? "Europees" : "Nationaal"}
- Winnaars: ${winnaarNamen(record).join(', ') || "niet bekend"}
- Gunningswaarde: ${record.gunningswaarde ? "€" + record.gunningswaarde : "niet bekend"}
- Teams binnen Regions & cities (AI): ${(c.teams || []).join(", ") || "geen onderbouwde match"}
- Units (AI): ${(c.units || []).join(", ") || "geen onderbouwde match"}
- Gevraagde prestatie (AI): ${c.opdrachtprofiel?.vraag || "niet uitgewerkt"}
- Thema (AI): ${c.thema || "—"}
- Methodiek (AI): ${c.methodiek || "—"}
`;
  }

  // Modal openen en reset
  const overlay = document.getElementById("chat-overlay");
  overlay.classList.add("open");
  document.getElementById("chat-titel").textContent = titel;
  document.getElementById("chat-synthese").innerHTML = `<span style="color:var(--text3)">Document laden...</span>`;
  document.getElementById("chat-messages").innerHTML = "";
  document.getElementById("chat-input").value = "";
  document.getElementById("workspace").inert = true;
  document.getElementById("chat-input").focus();

  // Document ophalen
  try {
    const resp = await fetch(`data/documenten/${pubId}.txt`);
    if (!resp.ok) throw new Error("niet gevonden");
    chatDocTekst = await resp.text();
  } catch(e) {
    document.getElementById("chat-synthese").innerHTML = `<span style="color:var(--ecorys-orange)">Geen aanbestedingsdocumenten beschikbaar voor deze opdracht.</span>`;
    return;
  }

  // Synthese genereren
  document.getElementById("chat-synthese").innerHTML = `<span style="color:var(--text3)">Synthese genereren...</span>`;
  const synthese = await vraagClaude(
    `Je bent een expert consultant assistent. Geef een beknopte synthese (max 150 woorden, gebruik markdown) van onderstaand aanbestedingsdocument. Dek: wat wordt gevraagd, welke methodiek, welke eisen, looptijd en waarde.\n\n${chatRecordMeta}\n\nDOCUMENT:\n${chatDocTekst.slice(0, 25000)}`,
    [{ role: "user", content: "Geef de synthese van dit aanbestedingsdocument." }]
  );
  document.getElementById("chat-synthese").innerHTML = htmlTekst(synthese).replace(/\*\*(.*?)\*\*/g,"<strong>$1</strong>").replace(/\n/g,"<br>");
}

async function stuurChatBericht() {
  const input = document.getElementById("chat-input");
  const vraag = input.value.trim();
  if (!vraag) return;
  input.value = "";

  // Gebruikersbericht tonen
  voegBerichtToe("user", vraag);
  chatGeschiedenis.push({ role: "user", content: vraag });

  // Typing indicator
  const typingId = "typing-" + Date.now();
  document.getElementById("chat-messages").insertAdjacentHTML("beforeend",
    `<div id="${typingId}" class="chat-typing">Claude denkt na...</div>`);
  scrollChat();

  document.getElementById("chat-send").disabled = true;

  const systeemContext = chatDocTekst
    ? `Je bent een assistent die vragen beantwoordt over een aanbestedingsdocument. Beantwoord vragen op basis van onderstaand document en metadata. Als de informatie niet beschikbaar is, zeg dat dan eerlijk.\n\n${chatRecordMeta}\n\nDOCUMENT:\n${chatDocTekst.slice(0, 25000)}`
    : `Je bent een assistent. Er zijn geen aanbestedingsdocumenten beschikbaar voor deze opdracht. Zeg dit eerlijk als er naar documentinhoud gevraagd wordt.`;

  const antwoord = await vraagClaude(systeemContext, chatGeschiedenis);

  // Typing verwijderen en antwoord tonen
  document.getElementById(typingId)?.remove();
  voegBerichtToe("assistant", antwoord);
  chatGeschiedenis.push({ role: "assistant", content: antwoord });
  document.getElementById("chat-send").disabled = false;
}

async function vraagClaude(systeem, geschiedenis) {
  // Korte pauze om rate limit te vermijden
  await new Promise(r => setTimeout(r, 1500));
  try {
    const resp = await fetch(AZURE_ENDPOINT, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": AZURE_API_KEY,
        "anthropic-version": "2023-06-01",
      },
      body: JSON.stringify({
        model: AZURE_MODEL,
        max_tokens: 2048,
        // Sonnet 5.5 denkt standaard adaptief mee; lage effort houdt de chat
        // snel en goedkoop zonder het denken helemaal uit te zetten.
        output_config: { effort: "low" },
        system: systeem,
        messages: geschiedenis,
      }),
    });
    const data = await resp.json();
    if (data.error) return `Fout: ${data.error.message}`;
    // Bij adaptief denken kan content[0] een thinking-blok zijn — pak het tekstblok.
    return data.content?.find(b => b.type === "text")?.text || "Geen antwoord ontvangen.";
  } catch(e) {
    return `Fout: ${e.message}`;
  }
}

function voegBerichtToe(rol, tekst) {
  const avatar = rol === "user" ? "JIJ" : "AI";
  const html = `<div class="chat-msg ${rol}">
    <div class="chat-avatar">${avatar}</div>
    <div class="chat-bubble">${htmlTekst(tekst).replace(/\n/g,"<br>")}</div>
  </div>`;
  document.getElementById("chat-messages").insertAdjacentHTML("beforeend", html);
  scrollChat();
}

function scrollChat() {
  const el = document.getElementById("chat-messages");
  el.scrollTop = el.scrollHeight;
}

function sluitModal() {
  document.getElementById("chat-overlay").classList.remove("open");
  document.getElementById("workspace").inert = false;
  document.querySelector("[data-action=chat]")?.focus();
}

// Enter om te versturen
document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("chat-input")?.addEventListener("keydown", e => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); stuurChatBericht(); }
  });
});
