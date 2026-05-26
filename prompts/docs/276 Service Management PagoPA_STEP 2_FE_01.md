# 276 Service Management PagoPA (STEP 2)
## Analisi Funzionale – Front End

**Classificazione:** RISERVATA  
**Dominio:** Nexi Payments SpA  
**Autore:** Morgan Bossi  
**Data:** 20/04/2026

---

## Scopo del documento
Definire l’analisi funzionale relativa alla funzionalità di **Ricerca Operazioni** nel cruscotto di Service Management.

---

## Sommario
- Ricerca Operazioni
- API disponibili
- Pagina 1 – Ricerche
- Pagina 2 – Dettagli
- Informazioni generali

---

# Ricerca Operazioni

Nel cruscotto di Service Management viene introdotta una nuova funzionalità di **Ricerca Operazioni**.

---

## API disponibili

### Ricerca posizioni debitorie
- `GET /search/nav/{nav}` → ricerca per Codice Avviso
- `GET /search/iuv/{iuv}` → ricerca per IUV
- `GET /search/cart/{id_cart}` → ricerca per ID Carrello
- `GET /search/token/{token}` → ricerca per Token
- `GET /search/extra/{searchValue}` → ricerca per dati aggiuntivi

### Visualizzazione posizione
- `GET /position/{nav}/{pa-emittente}`

### Dettagli
- `GET /token/${token}`
- `GET /transfers/{nav}/{pa-emittente}/${token}`
- `GET /workflows/${nav}/${pa-emittente}`
- `GET /extra/${token}`

---

## Parametri di ricerca

È possibile filtrare per:

### Facoltativo
- **PA Emittente** (CF a 11 cifre)

### Mutualmente esclusivi (uno solo valorizzabile)
- NAV
- IUV
- TOKEN
- ID Carrello
- Informazioni aggiuntive (RRN, Transaction ID, ecc.)

⚠️ Tutte le ricerche sono:
- case insensitive
- match **esatto**
- NON supportano LIKE

---

# Pagina 1 – Ricerche

## Descrizione
Pagina di input per:
- inserimento criteri di ricerca
- visualizzazione risultati

## Accesso
Consentito solo a utenti autorizzati

---

## Campi di ricerca

| Campo | Descrizione | API |
|------|------------|-----|
| PA Emittente | CF della PA | opzionale |
| Avviso (NAV) | Codice avviso | `/search/nav/{nav}` |
| IUV | Codice IUV | `/search/iuv/{iuv}` |
| Token | Token pagamento | `/search/token/{token}` |
| Id Carrello | ID carrello | `/search/cart/{id_cart}` |
| Info aggiuntive | Label/valore | `/search/extra/{searchValue}` |

---

## Risultati

Output principale:
- PA Emittente
- Numero Avviso

Solo per info aggiuntive:
- Tipo Info (match)
- Valore Info

### Requisiti UI
- mostrare colonne dinamiche
- evitare colonne vuote se non applicabili
- aggiungere **copia veloce (hover/click)** per identificativi

---

## Navigazione

- click su icona lente → apertura pagina dettaglio

---

# Pagina 2 – Dettagli

## Struttura

### Sezione superiore (header posizione)

Campi:
- nav
- pa-emittente
- iuv
- creditor-reference-id
- last-event

### Stato pagamento
- visualizzare label `PAGATA` se applicabile

---

## Sezioni principali

### TAB 1: Tokens
### TAB 2: Eventi

---

# Tokens

## Fonte dati
- lista token da `/position`
- dettaglio da `/token/${token}`

## Visualizzazione
- elenco base (tabella)
- dettaglio espandibile inline

---

## Dettaglio Token

### PAGAMENTO
- payment-born → registrazione pagamento
- payed-date → data pagamento
- is-payed-token → token pagato
- multi-outcome → TBD
- amount → importo
- fee → commissioni

### ATTORI
- PSP → id PSP
- Nome PSP
- Station
- pt-pa → partner PA
- Nome pt-pa
- pt-psp → partner PSP
- channel

### PAYMENT INFO
- touchpoint
- payment-method
- is-dw → TBD
- is-gpd
- is-standin
- is-cart

---

## Azioni Token

### 1. Elenco Transfer
- API: `/transfers/{nav}/{pa-emittente}/${token}`
- apertura sezione espandibile

### 2. Informazioni aggiuntive
- API: `/extra/${token}`
- apertura sezione espandibile

---

## Regole UI

- sezioni indipendenti (apertura multipla)
- ordine:
    1. dettaglio
    2. transfer
    3. info aggiuntive

- supporto multi-dettaglio simultaneo
- pulsante `X` per chiusura massiva

---

# Eventi

## Fonte dati
- API: `/workflows/${nav}/${pa-emittente}`

## Comportamento
- click su token:
    - switch a tab Tokens
    - apertura dettaglio token selezionato

---

# Informazioni Generali

## Versioning

| Versione | Data | Note |
|---------|------|-----|
| 00 | 08/04/2026 | Prima stesura |
| 01 | 20/04/2026 | Revisione PagoPA |

---

## Classificazione Documento

- PUBBLICA
- INTERNA
- RISERVATA
- STRETTAMENTE RISERVATA

---

## Note

- documento destinato a uso interno Nexi
- non implica approvazione esterna
- valido solo se firmato
