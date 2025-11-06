// server.js
const express = require('express');
const archiver = require('archiver');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs'); // presente per hardening futuro (POC: password in chiaro)
const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const validator = require('validator');
const path = require('path');
const fs = require('fs').promises;
require('dotenv').config();

// === Dati negozi (per ottenere codici PV) ===
const negozi = require('./data/negozi.json');

const app = express();
const PORT = process.env.PORT || 3001;

// === Credenziali Google locali opzionali ===
const localServiceAccountPath = path.join(__dirname, 'credentials', 'google-service-account.local.json');
let localServiceAccount;
try { localServiceAccount = require(localServiceAccountPath); } catch (e) { if (e.code !== 'MODULE_NOT_FOUND') console.warn(e.message); }

// === Diagnostica avvio ===
const hasGoogleEmailEnv = !!process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
const hasGoogleEmailLocal = !!localServiceAccount?.client_email;
const hasGoogleKeyEnv = !!process.env.GOOGLE_PRIVATE_KEY;
const hasGoogleKeyLocal = !!localServiceAccount?.private_key;
const googleEmailSource = hasGoogleEmailEnv ? 'env' : hasGoogleEmailLocal ? 'local file' : 'missing';
const googleKeySource = hasGoogleKeyEnv ? 'env' : hasGoogleKeyLocal ? 'local file' : 'missing';
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;

console.log('🔍 STARTUP');
console.log('📊 PORT:', PORT);
console.log('🔐 JWT_SECRET:', !!process.env.JWT_SECRET);
console.log('📊 GOOGLE_SHEET_ID:', GOOGLE_SHEET_ID ? 'OK' : 'MISSING');
console.log('🤖 GOOGLE_SERVICE_ACCOUNT_EMAIL:', googleEmailSource);
console.log('🔑 GOOGLE_PRIVATE_KEY:', googleKeySource);

// === Cartella file TXT ===
const TXT_FILES_DIR = path.join(__dirname, 'generated_txt_files');
const ensureTxtDir = async () => { try { await fs.access(TXT_FILES_DIR); } catch { await fs.mkdir(TXT_FILES_DIR, { recursive: true }); } };
ensureTxtDir().then(() => console.log('📁 TXT dir pronta:', TXT_FILES_DIR));

// === Middleware globali ===
app.use(helmet());
app.use(cors({ origin: true, credentials: true }));
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// === Google Auth ===
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || localServiceAccount?.client_email;
const rawGooglePrivateKey = process.env.GOOGLE_PRIVATE_KEY || localServiceAccount?.private_key;
// normalizza eventuali \n
const GOOGLE_PRIVATE_KEY = rawGooglePrivateKey ? rawGooglePrivateKey.replace(/\\n/g, '\n') : undefined;

if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY) {
  console.warn("⚠️ Credenziali Google incomplete: configura variabili d'ambiente o file locale.");
}

const serviceAccountAuth = new JWT({
  email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
  key: GOOGLE_PRIVATE_KEY,
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

const getGoogleDoc = async () => { const doc = new GoogleSpreadsheet(GOOGLE_SHEET_ID, serviceAccountAuth); await doc.loadInfo(); return doc; };
const getGoogleSheet = async (sheetName = null) => {
  try {
    const doc = await getGoogleDoc();
    if (sheetName) {
      const sheet = doc.sheetsByTitle[sheetName];
      if (!sheet) throw new Error(`Foglio "${sheetName}" non trovato`);
      return sheet;
    }
    return doc.sheetsByIndex[0];
  } catch (err) {
    console.error('Errore connessione Google Sheets:', err);
    throw new Error('Impossibile connettersi a Google Sheets');
  }
};

/* =========================================================
 *                     LOADERS DA SHEETS
 * =======================================================*/
const loadAllSheetData = async () => {
  const sheet = await getGoogleSheet();
  const rows = await sheet.getRows();
  const data = rows.map(row => ({
    id: row.get('id'),
    numero: row.get('numero'),
    fornitore: row.get('fornitore'),
    data_emissione: row.get('data_emissione'),
    data_consegna: row.get('data_consegna'),
    stato: row.get('stato'),
    punto_vendita: row.get('punto_vendita'),
    confermato_da: row.get('confermato_da'),
    pdf_link: row.get('pdf_link'),
    importo_totale: row.get('importo_totale'),
    note: row.get('note') || '',
    txt: row.get('txt') || '',
    codice_fornitore: row.get('codice_fornitore') || '',
    testo_ddt: row.get('testo_ddt') || '',
    // Colonna O (errore di conversione)
    item_noconv: row.get('item_noconv') || ''
  }));
  // rimuovi duplicati su id
  return data.filter((inv, i, self) => i === self.findIndex(x => x.id === inv.id));
};

const loadSheetData = async (puntoVendita) => {
  const sheet = await getGoogleSheet();
  const rows = await sheet.getRows();
  let data = rows.map(row => ({
    id: row.get('id'),
    numero: row.get('numero'),
    fornitore: row.get('fornitore'),
    data_emissione: row.get('data_emissione'),
    data_consegna: row.get('data_consegna'),
    stato: row.get('stato'),
    punto_vendita: row.get('punto_vendita'),
    confermato_da: row.get('confermato_da'),
    pdf_link: row.get('pdf_link'),
    importo_totale: row.get('importo_totale'),
    note: row.get('note') || '',
    txt: row.get('txt') || '',
    codice_fornitore: row.get('codice_fornitore') || '',
    testo_ddt: row.get('testo_ddt') || '',
    item_noconv: row.get('item_noconv') || ''
  }));
  if (puntoVendita) data = data.filter(r => r.punto_vendita === puntoVendita);
  return data;
};

const loadAllMovimentazioniData = async () => {
  const sheet = await getGoogleSheet('Movimentazioni');
  const rows = await sheet.getRows();
  const data = rows.map(row => ({
    id: row.get('id'),
    data_movimento: row.get('data_movimento'),
    timestamp: row.get('timestamp'),
    origine: row.get('origine'),
    codice_origine: row.get('codice_origine') || '',
    prodotto: row.get('prodotto'),
    quantita: row.get('quantita'),
    unita_misura: row.get('unita_misura'),
    destinazione: row.get('destinazione'),
    codice_destinazione: row.get('codice_destinazione') || '',
    stato: row.get('stato') || 'registrato',
    txt_content: row.get('txt_content') || '',
    txt_filename: row.get('txt_filename') || '',
    creato_da: row.get('creato_da') || ''
  }));
  data.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
  return data;
};

/* =========================================================
 *                   GENERAZIONE FILE TXT
 *   (marca _ERRORI se note oppure item_noconv sono valorizzati)
 * =======================================================*/
const generateTxtFile = async (invoiceData) => {
  const numeroDocumento = invoiceData.numero;
  const dataConsegna = invoiceData.data_consegna;
  const nomeFornitore = invoiceData.fornitore;
  const puntoVendita = invoiceData.punto_vendita;
  const contenutoTxt = invoiceData.txt || '';
  const noteErrori = invoiceData.note || '';
  const itemNoConv = invoiceData.item_noconv || '';

  const negozio = negozi.find(n => n.nome === puntoVendita);
  const codicePV = negozio?.codice || 'UNKNOWN';

  if (!numeroDocumento || !dataConsegna || !nomeFornitore) return null;
  if (!contenutoTxt.trim()) return null;

  const clean = (s) => String(s).replace(/[\/\\:*?"<>|]/g, '_').replace(/\s+/g, '_').replace(/_{2,}/g, '_').trim();
  const hasErrors = (!!noteErrori && noteErrori.trim() !== '') || (!!itemNoConv && itemNoConv.trim() !== '');
  const errorSuffix = hasErrors ? '_ERRORI' : '';

  const fileName = `${clean(numeroDocumento)}_${dataConsegna}_${clean(nomeFornitore)}_${clean(codicePV)}${errorSuffix}.txt`;
  const filePath = path.join(TXT_FILES_DIR, fileName);

  await fs.writeFile(filePath, contenutoTxt, 'utf8');
  console.log(hasErrors ? `⚠️ TXT con errori: ${fileName}` : `✅ TXT: ${fileName}`);
  return { fileName, filePath, size: contenutoTxt.length, hasErrors, noteErrori: noteErrori || null, item_noconv: itemNoConv || null };
};

/* =========================================================
 *                         AUTH
 * =======================================================*/
const users = [
  // UFFICIO CENTRALE
  { id: 1,   name: "FDV Office", email: "office@fradiavolopizzeria.com", password: "fdv2025", puntoVendita: "FDV Office", role: "admin" },
  { id: 999, name: "Admin Fradiavolo", email: "admin@fradiavolopizzeria.com", password: "admin2025", puntoVendita: "ADMIN_GLOBAL", role: "admin", permissions: ["view_all","edit_all","manage_users","analytics","reports","system_config"], storeAccess: 'global' },

  // GENOVA
  { id: 101, name: "FDV Genova Castello", email: "genova.castello@fradiavolopizzeria.com", password: "castello2025", puntoVendita: "FDV Genova Castello", role: "operator" },
  { id: 128, name: "FDV Genova Mare", email: "genova.mare@fradiavolopizzeria.com", password: "mare2025", puntoVendita: "FDV Genova Mare", role: "operator" },

  // MILANO
  { id: 113, name: "FDV Milano Sempione", email: "milano.sempione@fradiavolopizzeria.com", password: "sempione2025", puntoVendita: "FDV Milano Sempione", role: "operator" },
  { id: 120, name: "FDV Milano Isola", email: "milano.isola@fradiavolopizzeria.com", password: "isola2025", puntoVendita: "FDV Milano Isola", role: "operator" },
  { id: 121, name: "FDV Milano Citylife", email: "milano.citylife@fradiavolopizzeria.com", password: "citylife2025", puntoVendita: "FDV Milano Citylife", role: "operator" },
  { id: 125, name: "FDV Milano Bicocca", email: "milano.bicocca@fradiavolopizzeria.com", password: "bicocca2025", puntoVendita: "FDV Milano Bicocca", role: "operator" },
  { id: 127, name: "FDV Milano Premuda", email: "milano.premuda@fradiavolopizzeria.com", password: "premuda2025", puntoVendita: "FDV Milano Premuda", role: "operator" },
  { id: 131, name: "FDV Milano Porta Venezia", email: "milano.portavenezia@fradiavolopizzeria.com", password: "portavenezia2025", puntoVendita: "FDV Milano Porta Venezia", role: "operator" },

  // TORINO
  { id: 114, name: "FDV Torino Carlina", email: "torino.carlina@fradiavolopizzeria.com", password: "carlina2025", puntoVendita: "FDV Torino Carlina", role: "operator" },
  { id: 117, name: "FDV Torino GM", email: "torino.gm@fradiavolopizzeria.com", password: "gm2025", puntoVendita: "FDV Torino GM", role: "operator" },
  { id: 123, name: "FDV Torino IV Marzo", email: "torino.ivmarzo@fradiavolopizzeria.com", password: "ivmarzo2025", puntoVendita: "FDV Torino IV Marzo", role: "operator" },
  { id: 130, name: "FDV Torino Vanchiglia", email: "torino.vanchiglia@fradiavolopizzeria.com", password: "vanchiglia2025", puntoVendita: "FDV Torino Vanchiglia", role: "operator" },
  { id: 136, name: "FDV Torino San Salvario", email: "torino.sansalvario@fradiavolopizzeria.com", password: "sansalvario2025", puntoVendita: "FDV Torino San Salvario", role: "operator" },

  // ROMA
  { id: 107, name: "FDV Roma Parioli", email: "roma.parioli@fradiavolopizzeria.com", password: "parioli2025", puntoVendita: "FDV Roma Parioli", role: "operator" },
  { id: 133, name: "FDV Roma Ostiense", email: "roma.ostiense@fradiavolopizzeria.com", password: "ostiense2025", puntoVendita: "FDV Roma Ostiense", role: "operator" },
  { id: 138, name: "FDV Roma Trastevere", email: "roma.trastevere@fradiavolopizzeria.com", password: "trastevere2025", puntoVendita: "FDV Roma Trastevere", role: "operator" },

  // EMILIA ROMAGNA
  { id: 106, name: "FDV Bologna S.Stefano", email: "bologna.stefano@fradiavolopizzeria.com", password: "stefano2025", puntoVendita: "FDV Bologna S.Stefano", role: "operator" },
  { id: 124, name: "FDV Parma", email: "parma@fradiavolopizzeria.com", password: "parma2025", puntoVendita: "FDV Parma", role: "operator" },
  { id: 132, name: "FDV Modena", email: "modena@fradiavolopizzeria.com", password: "modena2025", puntoVendita: "FDV Modena", role: "operator" },
  { id: 137, name: "FDV Rimini", email: "rimini@fradiavolopizzeria.com", password: "rimini2025", puntoVendita: "FDV Rimini", role: "operator" },

  // LOMBARDIA
  { id: 122, name: "FDV Arese", email: "arese@fradiavolopizzeria.com", password: "arese2025", puntoVendita: "FDV Arese", role: "operator" },
  { id: 126, name: "FDV Monza", email: "monza@fradiavolopizzeria.com", password: "monza2025", puntoVendita: "FDV Monza", role: "operator" },
  { id: 135, name: "FDV Brescia Centro", email: "brescia.centro@fradiavolopizzeria.com", password: "brescia2025", puntoVendita: "FDV Brescia Centro", role: "operator" },

  // PIEMONTE
  { id: 112, name: "FDV Novara", email: "novara@fradiavolopizzeria.com", password: "novara2025", puntoVendita: "FDV Novara", role: "operator" },
  { id: 129, name: "FDV Alessandria", email: "alessandria@fradiavolopizzeria.com", password: "alessandria2025", puntoVendita: "FDV Alessandria", role: "operator" },
  { id: 134, name: "FDV Asti", email: "asti@fradiavolopizzeria.com", password: "asti2025", puntoVendita: "FDV Asti", role: "operator" },

  // ALTRE REGIONI
  { id: 119, name: "FDV Varese", email: "varese@fradiavolopizzeria.com", password: "varese2025", puntoVendita: "FDV Varese", role: "operator" }
];

const authenticateToken = (req, res, next) => {
  const token = (req.headers['authorization'] || '').split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Token di accesso richiesto' });
  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ error: 'Token non valido' });
    req.user = user; next();
  });
};
const requireAdmin = (req, res, next) => (req.user.role !== 'admin' ? res.status(403).json({ error: 'Accesso riservato agli amministratori' }) : next());
const validateEmail = (email) => validator.isEmail(email) && (email.includes('@fradiavolopizzeria.com') || email.includes('@azienda.it'));
const validateDate = (ds) => validator.isDate(ds) && new Date(ds) <= new Date();
const sanitizeInput = (i) => validator.escape(String(i ?? '').trim());

// === AUTH ROUTES ===
app.post('/api/auth/login', rateLimit({ windowMs: 15 * 60 * 1000, max: 5 }), async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ error: 'Email e password richiesti' });
    if (!validateEmail(email)) return res.status(400).json({ error: 'Email non valida' });

    const user = users.find(u => u.email === email);
    if (!user) return res.status(401).json({ error: 'Credenziali non valide' });
    const isValidPassword = password === user.password; // POC
    if (!isValidPassword) return res.status(401).json({ error: 'Credenziali non valide' });

    if (!process.env.JWT_SECRET) return res.status(500).json({ error: 'JWT_SECRET non configurato' });

    const tokenPayload = { userId: user.id, email: user.email, puntoVendita: user.puntoVendita, role: user.role };
    const token = jwt.sign(tokenPayload, process.env.JWT_SECRET, { expiresIn: '8h' });
    res.json({ success: true, token, user: { id: user.id, name: user.name, email: user.email, puntoVendita: user.puntoVendita, role: user.role } });
  } catch (e) {
    console.error('login error:', e);
    res.status(500).json({ error: 'Errore interno del server' });
  }
});
app.get('/api/auth/verify', authenticateToken, (req, res) => {
  const user = users.find(u => u.id === req.user.userId);
  if (!user) return res.status(401).json({ error: 'Utente non trovato' });
  res.json({ success: true, user: { id: user.id, name: user.name, email: user.email, puntoVendita: user.puntoVendita, role: user.role } });
});
app.post('/api/auth/logout', authenticateToken, (req, res) => res.json({ success: true, message: 'Logout effettuato' }));

/* =========================================================
 *                 PRODOTTI (da Google Sheet dedicato)
 * =======================================================*/
const loadProdottiFromSheet = async () => {
  try {
    const PRODOTTI_SHEET_ID = '1CJhd14F8qV8nS0-SK2ENSNSkWaE21KotK2ArBjJETfk';
    const prodottiDoc = new GoogleSpreadsheet(PRODOTTI_SHEET_ID, serviceAccountAuth);
    await prodottiDoc.loadInfo();
    const sheet = prodottiDoc.sheetsByTitle?.['PRODOTTI'] || prodottiDoc.sheetsByIndex[0];
    if (!sheet) throw new Error('Tab prodotti non trovato');

    const rows = await sheet.getRows();
    const norm = (v) => (v ?? '').toString().trim();
    const prodotti = rows.map(r => ({
      nome:   norm(r.get('Nome') || r.get('DESCRIZIONE') || r.get('Descrizione') || r.get('nome')),
      codice: norm(r.get('Cod.mago') || r.get('CODICE') || r.get('Codice') || r.get('SKU') || r.get('codice')),
      unitaMisura: norm(r.get('UMB') || r.get('UM') || r.get('UOM') || r.get('unita_misura')),
      onOff:  norm(r.get('On.Off') ?? r.get('On') ?? r.get('Attivo') ?? r.get('active')),
      brand:  norm(r.get('Marca') || r.get('Brand') || r.get('brand')),
      pack:   norm(r.get('Confezione') || r.get('Pack') || r.get('Formato')),
      materiale: norm(r.get('Materiale') || r.get('Imballo') || r.get('Package'))
    }));
    return prodotti.filter(p => p.nome);
  } catch (error) {
    console.error('prodotti load error:', error);
    try {
      const prodottiData = require('../frontend/src/data/prodotti.json');
      return Array.isArray(prodottiData) ? prodottiData : [];
    } catch {
      return [];
    }
  }
};

app.get('/api/prodotti', authenticateToken, async (req, res) => {
  try {
    const { active, page, per_page } = req.query;
    const all = await loadProdottiFromSheet();
    const onlyActive = active === '1';
    let filtered = onlyActive ? all.filter(p => ['1', 1, true, 'TRUE', 'true'].includes(p.onOff)) : all;

    filtered.sort((a, b) => (a.nome || '').localeCompare(b.nome || '', 'it', { sensitivity: 'base' }));

    const total = filtered.length;
    let data = filtered;
    let nextPage = null;

    const perPageNum = Math.max(parseInt(per_page || '0', 10), 0);
    const pageNum = Math.max(parseInt(page || '1', 10), 1);

    if (perPageNum > 0) {
      const start = (pageNum - 1) * perPageNum;
      const end = start + perPageNum;
      data = filtered.slice(start, end);
      if (end < total) nextPage = pageNum + 1;

      res.set('X-Total-Count', String(total));
      if (nextPage) {
        const base = req.originalUrl.split('?')[0];
        res.set('Link', `<${base}?page=${nextPage}&per_page=${perPageNum}${onlyActive ? '&active=1' : ''}>; rel="next"`);
      }
    }

    res.json({ success: true, sheet: 'PRODOTTI', total, returned: data.length, nextPage, data });
  } catch (error) {
    console.error('prodotti api error:', error);
    res.status(500).json({ error: 'Impossibile caricare la lista prodotti', details: error.message });
  }
});

/* =========================================================
 *             INVOICES: UPDATE + GENERAZIONE TXT
 * =======================================================*/
const updateSheetRow = async (id, updates) => {
  const sheet = await getGoogleSheet();
  const rows = await sheet.getRows();
  const row = rows.find(r => r.get('id') === id.toString());
  if (!row) throw new Error('Fattura non trovata');

  // snapshot prima dell’update per generare TXT
  const invoiceDataForTxt = {
    id: row.get('id'),
    numero: row.get('numero'),
    fornitore: row.get('fornitore'),
    data_emissione: row.get('data_emissione'),
    data_consegna: updates.data_consegna || row.get('data_consegna'),
    punto_vendita: row.get('punto_vendita'),
    confermato_da: updates.confermato_da || row.get('confermato_da'),
    txt: row.get('txt') || '',
    codice_fornitore: row.get('codice_fornitore') || '',
    note: updates.note ?? row.get('note') || '',
    // include colonna O (errore di conversione)
    item_noconv: row.get('item_noconv') || ''
  };

  Object.keys(updates).forEach(k => row.set(k, updates[k]));
  await row.save();

  if (updates.stato === 'consegnato') {
    try { await generateTxtFile(invoiceDataForTxt); } catch (e) { console.error('gen txt error:', e); }
  }
  return true;
};

// Invoices - list (user vs admin)
app.get('/api/invoices', authenticateToken, async (req, res) => {
  try {
    const puntoVendita = req.user.role === 'admin' ? null : req.user.puntoVendita;
    const data = req.user.role === 'admin' ? await loadAllSheetData() : await loadSheetData(puntoVendita);
    res.json({ success: true, data });
  } catch (error) {
    console.error('invoices list error:', error);
    res.status(500).json({ error: 'Impossibile caricare le fatture' });
  }
});

// Conferma consegna -> genera TXT
app.post('/api/invoices/:id/confirm', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { data_consegna, note_errori, confermato_da_email } = req.body;

    if (!id || !data_consegna) return res.status(400).json({ error: 'ID fattura e data consegna richiesti' });
    if (!validateDate(data_consegna)) return res.status(400).json({ error: 'Data non valida' });

    let confermatoDa = req.user.email;
    if (req.user.role === 'admin' && confermato_da_email && validateEmail(confermato_da_email)) {
      confermatoDa = sanitizeInput(confermato_da_email);
    }

    const updates = { stato: 'consegnato', data_consegna: sanitizeInput(data_consegna), confermato_da: confermatoDa };
    if (note_errori && note_errori.trim()) updates.note = sanitizeInput(note_errori.trim());

    await updateSheetRow(id, updates);
    res.json({ success: true, message: 'Consegna confermata' });
  } catch (error) {
    console.error('confirm error:', error);
    res.status(500).json({ error: 'Impossibile confermare la consegna' });
  }
});

// Update generico fattura
app.put('/api/invoices/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { data_consegna, confermato_da, note } = req.body;
    if (!id) return res.status(400).json({ error: 'ID fattura richiesto' });

    const updates = {};
    if (data_consegna) {
      if (!validateDate(data_consegna)) return res.status(400).json({ error: 'Data non valida' });
      updates.data_consegna = sanitizeInput(data_consegna);
    }
    if (confermato_da) {
      if (!validateEmail(confermato_da)) return res.status(400).json({ error: 'Email non valida' });
      updates.confermato_da = sanitizeInput(confermato_da);
    }
    if (note !== undefined) updates.note = sanitizeInput(note);
    if (Object.keys(updates).length === 0) return res.status(400).json({ error: 'Nessun campo da aggiornare' });

    await updateSheetRow(id, updates);
    res.json({ success: true, message: 'Fattura aggiornata', updated_fields: Object.keys(updates) });
  } catch (error) {
    console.error('invoice update error:', error);
    res.status(500).json({ error: 'Impossibile aggiornare la fattura: ' + error.message });
  }
});

/* =========================================================
 *                     MOVIMENTAZIONI
 * =======================================================*/
const saveMovimentazioniToSheet = async (movimenti, origine) => {
  let sheet;
  try {
    sheet = await getGoogleSheet('Movimentazioni');
  } catch {
    const doc = await getGoogleDoc();
    sheet = await doc.addSheet({
      title: 'Movimentazioni',
      headerValues: [
        'id','data_movimento','timestamp','origine','codice_origine',
        'prodotto','quantita','unita_misura','destinazione','codice_destinazione',
        'stato','txt_content','txt_filename','creato_da'
      ]
    });
  }

  const timestamp = new Date().toISOString();
  const dataOggi = new Date().toLocaleDateString('it-IT');

  const righe = movimenti.map((movimento, index) => ({
    id: `${timestamp}-${index}`,
    data_movimento: dataOggi,
    timestamp,
    origine: movimento.origine || origine,
    codice_origine: movimento.codice_origine || '',
    prodotto: movimento.prodotto,
    quantita: movimento.quantita,
    unita_misura: movimento.unita_misura,
    destinazione: movimento.destinazione,
    codice_destinazione: movimento.codice_destinazione || '',
    stato: 'registrato',
    txt_content: movimento.txt_content || '',
    txt_filename: movimento.txt_filename || '',
    creato_da: movimento.creato_da || ''
  }));

  await sheet.addRows(righe);
  return { success: true, righe_inserite: righe.length };
};

app.get('/api/movimentazioni', authenticateToken, async (req, res) => {
  try { res.json({ success: true, data: await loadAllMovimentazioniData() }); }
  catch (e) { console.error(e); res.status(500).json({ error: 'Impossibile caricare le movimentazioni' }); }
});

app.post('/api/movimentazioni', authenticateToken, async (req, res) => {
  try {
    const { movimenti, origine, creato_da_email } = req.body;
    if (!movimenti || !Array.isArray(movimenti) || movimenti.length === 0) return res.status(400).json({ error: 'Lista movimenti richiesta' });
    if (!origine || origine.trim() === '') return res.status(400).json({ error: 'Punto vendita di origine richiesto' });
    if (req.user.puntoVendita !== origine && req.user.role !== 'admin') return res.status(403).json({ error: 'Non autorizzato per questo punto vendita' });

    let customOrigin = origine;
    let customCreatoDa = req.user.email;
    if (req.user.role === 'admin' && creato_da_email && validateEmail(creato_da_email)) {
      const userObj = users.find(u => u.email === creato_da_email);
      if (userObj) { customOrigin = userObj.puntoVendita; customCreatoDa = userObj.email; }
    }

    for (let i = 0; i < movimenti.length; i++) {
      const m = movimenti[i];
      if (!m.prodotto || m.prodotto.trim() === '') return res.status(400).json({ error: `Prodotto richiesto per movimento ${i+1}` });
      if (!m.quantita || isNaN(m.quantita) || m.quantita <= 0) return res.status(400).json({ error: `Quantità valida richiesta per movimento ${i+1}` });
      if (!m.destinazione || m.destinazione.trim() === '') return res.status(400).json({ error: `Destinazione richiesta per movimento ${i+1}` });

      m.prodotto = sanitizeInput(m.prodotto);
      m.quantita = parseFloat(m.quantita);
      m.unita_misura = sanitizeInput(m.unita_misura || '');
      m.destinazione = sanitizeInput(m.destinazione);
      m.txt_content = m.txt_content || '';
      m.creato_da = customCreatoDa;
      m.origine = customOrigin;
    }

    const result = await saveMovimentazioniToSheet(movimenti, sanitizeInput(customOrigin));
    res.json({ success: true, message: `✅ ${result.righe_inserite} movimenti registrati con successo`, data: result });
  } catch (error) {
    console.error('movimentazioni post error:', error);
    res.status(500).json({ error: 'Impossibile salvare le movimentazioni' });
  }
});

/* =========================================================
 *                         ADMIN
 * =======================================================*/
app.get('/api/admin/dashboard', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const [invoices, movimentazioni] = await Promise.all([loadAllSheetData(), loadAllMovimentazioniData()]);
    const stats = {
      invoices: {
        total: invoices.length,
        consegnate: invoices.filter(inv => inv.stato === 'consegnato').length,
        pending: invoices.filter(inv => inv.stato === 'pending').length,
        byStore: {},
        byStatus: {},
        recentActivity: invoices.filter(inv => inv.data_consegna).sort((a,b)=>new Date(b.data_consegna)-new Date(a.data_consegna)).slice(0,10)
      },
      movimentazioni: {
        total: movimentazioni.length,
        thisMonth: movimentazioni.filter(m => { const d=new Date(m.timestamp); const n=new Date(); return d.getMonth()===n.getMonth() && d.getFullYear()===n.getFullYear(); }).length,
        byStore: {},
        recentActivity: movimentazioni.slice(0,10)
      }
    };
    invoices.forEach(inv => {
      if (inv.punto_vendita) {
        if (!stats.invoices.byStore[inv.punto_vendita]) stats.invoices.byStore[inv.punto_vendita] = { total:0, consegnate:0, pending:0 };
        stats.invoices.byStore[inv.punto_vendita].total++;
        if (inv.stato==='consegnato') stats.invoices.byStore[inv.punto_vendita].consegnate++;
        if (inv.stato==='pending') stats.invoices.byStore[inv.punto_vendita].pending++;
      }
      if (!stats.invoices.byStatus[inv.stato]) stats.invoices.byStatus[inv.stato]=0;
      stats.invoices.byStatus[inv.stato]++;
    });
    movimentazioni.forEach(m => {
      if (m.origine) stats.movimentazioni.byStore[m.origine]=(stats.movimentazioni.byStore[m.origine]||0)+1;
    });
    res.json({ success:true, stats });
  } catch (e) { console.error(e); res.status(500).json({ error:'Impossibile caricare la dashboard admin' }); }
});

app.get('/api/admin/invoices', authenticateToken, requireAdmin, async (req, res) => {
  try {
    let data = await loadAllSheetData();
    const { store, status, dateFrom, dateTo } = req.query;
    if (store && store!=='ALL') data = data.filter(inv => inv.punto_vendita === store);
    if (status && status!=='ALL') data = data.filter(inv => inv.stato === status);
    if (dateFrom) data = data.filter(inv => inv.data_emissione >= dateFrom);
    if (dateTo) data = data.filter(inv => inv.data_emissione <= dateTo);
    res.json({ success:true, data });
  } catch (e) { console.error(e); res.status(500).json({ error:'Impossibile caricare le fatture globali' }); }
});

app.get('/api/admin/movimentazioni', authenticateToken, requireAdmin, async (req, res) => {
  try {
    let data = await loadAllMovimentazioniData();
    const { store, dateFrom, dateTo } = req.query;
    if (store && store!=='ALL') data = data.filter(mov => mov.origine === store);
    if (dateFrom) data = data.filter(mov => mov.data_movimento >= dateFrom);
    if (dateTo) data = data.filter(mov => mov.data_movimento <= dateTo);
    res.json({ success:true, data });
  } catch (e) { console.error(e); res.status(500).json({ error:'Impossibile caricare le movimentazioni globali' }); }
});

app.get('/api/admin/stores', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const [invoices, movimentazioni] = await Promise.all([loadAllSheetData(), loadAllMovimentazioniData()]);
    const storeStats = {};
    invoices.forEach(inv => {
      if (inv.punto_vendita) {
        if (!storeStats[inv.punto_vendita]) storeStats[inv.punto_vendita] = { name: inv.punto_vendita, invoices:0, movimentazioni:0, lastActivity:null };
        storeStats[inv.punto_vendita].invoices++;
        const d = new Date(inv.data_consegna || inv.data_emissione);
        if (!storeStats[inv.punto_vendita].lastActivity || d > storeStats[inv.punto_vendita].lastActivity) storeStats[inv.punto_vendita].lastActivity = d;
      }
    });
    movimentazioni.forEach(mov => {
      if (mov.origine) {
        if (!storeStats[mov.origine]) storeStats[mov.origine] = { name: mov.origine, invoices:0, movimentazioni:0, lastActivity:null };
        storeStats[mov.origine].movimentazioni++;
        const d = new Date(mov.timestamp);
        if (!storeStats[mov.origine].lastActivity || d > storeStats[mov.origine].lastActivity) storeStats[mov.origine].lastActivity = d;
      }
    });
    const stores = Object.values(storeStats).sort((a,b)=>a.name.localeCompare(b.name));
    res.json({ success:true, stores });
  } catch (e) { console.error(e); res.status(500).json({ error:'Impossibile caricare i negozi' }); }
});

app.get('/api/admin/users', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const safeUsers = users.map(u => ({ id:u.id, name:u.name, email:u.email, puntoVendita:u.puntoVendita, role:u.role, permissions:u.permissions || [] }));
    res.json({ success:true, users: safeUsers });
  } catch (e) { console.error(e); res.status(500).json({ error:'Impossibile caricare gli utenti' }); }
});

app.get('/api/admin/export', authenticateToken, requireAdmin, async (req, res) => {
  try {
    const { type, format } = req.query;
    const data = {};
    if (type === 'invoices' || type === 'all') data.invoices = await loadAllSheetData();
    if (type === 'movimentazioni' || type === 'all') data.movimentazioni = await loadAllMovimentazioniData();

    if (format === 'csv') {
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="fradiavolo_export_${new Date().toISOString().split('T')[0]}.csv"`);
      res.send('CSV export non ancora implementato');
    } else {
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Content-Disposition', `attachment; filename="fradiavolo_export_${new Date().toISOString().split('T')[0]}.json"`);
      res.json({ exportDate: new Date().toISOString(), exportedBy: req.user.email, data });
    }
  } catch (e) { console.error(e); res.status(500).json({ error:'Impossibile completare l\'export' }); }
});

/* =========================================================
 *                        TXT FILES
 * =======================================================*/
app.get('/api/txt-files', authenticateToken, async (req, res) => {
  try {
    const files = await fs.readdir(TXT_FILES_DIR);
    const txtFiles = files.filter(file => file.endsWith('.txt') && !file.includes('.backup'));
    const fileList = await Promise.all(
      txtFiles.map(async (fileName) => {
        const filePath = path.join(TXT_FILES_DIR, fileName);
        const stats = await fs.stat(filePath);
        return { name: fileName, size: stats.size, created: stats.birthtime, modified: stats.mtime };
      })
    );
    res.json({ success: true, files: fileList.sort((a, b) => new Date(b.created) - new Date(a.created)) });
  } catch (error) {
    console.error('txt-files list error:', error);
    res.status(500).json({ error: 'Impossibile caricare la lista dei file TXT' });
  }
});

app.get('/api/txt-files/:filename', authenticateToken, async (req, res) => {
  try {
    const { filename } = req.params;
    if (!filename.endsWith('.txt') || filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: 'Nome file non valido' });
    }
    const filePath = path.join(TXT_FILES_DIR, filename);
    await fs.access(filePath);
    const fileContent = await fs.readFile(filePath, 'utf8');

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(fileContent);
  } catch (error) {
    if (error.code === 'ENOENT') return res.status(404).json({ error: 'File non trovato' });
    console.error('txt download error:', error);
    res.status(500).json({ error: 'Impossibile scaricare il file TXT' });
  }
});

// Lettura contenuto + rinomina automatica se deve avere _ERRORI
app.get('/api/txt-files/:filename/content', authenticateToken, async (req, res) => {
  try {
    let { filename } = req.params;
    if (!filename.endsWith('.txt') || filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: 'Nome file non valido' });
    }

    // NUMERO_YYYY-MM-DD_...
    const numeroDoc = filename.split('_')[0];
    const allInvoices = await loadAllSheetData();
    const related = allInvoices.find(inv => inv.numero === numeroDoc);

    const note_errori = related?.note || '';
    const item_noconv = related?.item_noconv || '';
    const shouldHaveErrorSuffix = (!!note_errori && note_errori.trim() !== '') || (!!item_noconv && item_noconv.trim() !== '');

    const currentlyHasSuffix = filename.includes('_ERRORI.txt');
    let renamedTo = null;

    if (shouldHaveErrorSuffix && !currentlyHasSuffix) {
      const newFilename = filename.replace(/\.txt$/i, '_ERRORI.txt');
      try {
        const oldPath = path.join(TXT_FILES_DIR, filename);
        const newPath = path.join(TXT_FILES_DIR, newFilename);
        await fs.access(oldPath);
        await fs.rename(oldPath, newPath);
        filename = newFilename;
        renamedTo = newFilename;
      } catch (e) {
        console.warn('Rinomina _ERRORI fallita:', e.message);
      }
    }

    const filePath = path.join(TXT_FILES_DIR, filename);
    await fs.access(filePath);
    const fileContent = await fs.readFile(filePath, 'utf8');

    const hasErrors = filename.includes('_ERRORI') || shouldHaveErrorSuffix;

    res.json({
      success: true,
      filename,
      renamedTo,
      content: fileContent,
      size: fileContent.length,
      hasErrors,
      errorDetails: related ? {
        note_errori: note_errori || undefined,       // "Errore segnalato in consegna"
        item_noconv: item_noconv || undefined,       // "Errore di conversione"
        data_consegna: related.data_consegna,
        confermato_da: related.confermato_da,
        fornitore: related.fornitore,
        numero: related.numero
      } : null
    });
  } catch (error) {
    if (error.code === 'ENOENT') return res.status(404).json({ error: 'File non trovato' });
    console.error('txt content error:', error);
    res.status(500).json({ error: 'Impossibile leggere il contenuto del file TXT' });
  }
});

app.put('/api/txt-files/:filename/content', authenticateToken, async (req, res) => {
  try {
    const { filename } = req.params;
    const { content } = req.body;
    if (!filename.endsWith('.txt') || filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: 'Nome file non valido' });
    }
    if (typeof content !== 'string') return res.status(400).json({ error: 'Contenuto deve essere una stringa' });

    const filePath = path.join(TXT_FILES_DIR, filename);
    await fs.access(filePath);

    const backupPath = path.join(TXT_FILES_DIR, `${filename}.backup.${Date.now()}`);
    const originalContent = await fs.readFile(filePath, 'utf8');
    await fs.writeFile(backupPath, originalContent, 'utf8');

    await fs.writeFile(filePath, content, 'utf8');
    res.json({ success: true, message: 'File aggiornato con successo', filename, size: content.length, backup_created: true });
  } catch (error) {
    if (error.code === 'ENOENT') return res.status(404).json({ error: 'File non trovato' });
    console.error('txt update error:', error);
    res.status(500).json({ error: 'Impossibile aggiornare il file TXT' });
  }
});

app.delete('/api/txt-files/:filename', authenticateToken, async (req, res) => {
  try {
    const { filename } = req.params;
    if (!filename.endsWith('.txt') || filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: 'Nome file non valido' });
    }
    const filePath = path.join(TXT_FILES_DIR, filename);
    await fs.access(filePath);

    const backupPath = path.join(TXT_FILES_DIR, `DELETED_${filename}.backup.${Date.now()}`);
    const originalContent = await fs.readFile(filePath, 'utf8');
    await fs.writeFile(backupPath, originalContent, 'utf8');
    await fs.unlink(filePath);

    res.json({ success: true, message: 'File eliminato con successo', filename, backup_created: true });
  } catch (error) {
    if (error.code === 'ENOENT') return res.status(404).json({ error: 'File non trovato' });
    console.error('txt delete error:', error);
    res.status(500).json({ error: 'Impossibile eliminare il file TXT' });
  }
});

// ZIP per data
app.get('/api/txt-files/download-by-date/:date', authenticateToken, async (req, res) => {
  try {
    const { date } = req.params;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return res.status(400).json({ error: 'Formato data non valido. Usa YYYY-MM-DD' });

    const allFiles = await fs.readdir(TXT_FILES_DIR);
    const txtFiles = allFiles.filter(file => file.endsWith('.txt') && !file.includes('.backup'));
    const filesForDate = txtFiles.filter(filename => {
      const m = filename.match(/(\d{4}-\d{2}-\d{2})/);
      return m && m[1] === date;
    });

    if (filesForDate.length === 0) return res.status(404).json({ error: `Nessun file TXT trovato per la data ${date}` });

    const archive = archiver('zip', { zlib: { level: 9 } });
    const zipFilename = `TXT_Files_${date}.zip`;
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="${zipFilename}"`);
    archive.pipe(res);

    for (const filename of filesForDate) {
      const filePath = path.join(TXT_FILES_DIR, filename);
      try {
        await fs.access(filePath);
        const fileContent = await fs.readFile(filePath, 'utf8');
        archive.append(fileContent, { name: filename });
      } catch (fileError) {
        console.warn('⚠️ Errore lettura file:', filename, fileError.message);
      }
    }
    await archive.finalize();
  } catch (error) {
    console.error('zip error:', error);
    if (!res.headersSent) res.status(500).json({ error: 'Impossibile creare l\'archivio ZIP: ' + error.message });
  }
});

// Statistiche per data
app.get('/api/txt-files/stats-by-date', authenticateToken, async (req, res) => {
  try {
    const allFiles = await fs.readdir(TXT_FILES_DIR);
    const txtFiles = allFiles.filter(file => file.endsWith('.txt') && !file.includes('.backup'));

    const filesByDate = {};
    for (const filename of txtFiles) {
      const m = filename.match(/(\d{4}-\d{2}-\d{2})/);
      if (!m) continue;
      const datePart = m[1];
      if (!filesByDate[datePart]) filesByDate[datePart] = [];

      const filePath = path.join(TXT_FILES_DIR, filename);
      const stats = await fs.stat(filePath);
      filesByDate[datePart].push({ name: filename, size: stats.size, created: stats.birthtime, modified: stats.mtime });
    }

    const sortedDates = Object.keys(filesByDate).sort((a, b) => new Date(b) - new Date(a));
    const dateGroups = sortedDates.map(date => ({
      date,
      fileCount: filesByDate[date].length,
      totalSize: filesByDate[date].reduce((sum, f) => sum + f.size, 0),
      files: filesByDate[date].sort((a, b) => new Date(b.created) - new Date(a.created))
    }));

    res.json({ success: true, totalFiles: txtFiles.length, totalDates: dateGroups.length, dateGroups });
  } catch (error) {
    console.error('stats error:', error);
    res.status(500).json({ error: 'Impossibile calcolare le statistiche: ' + error.message });
  }
});

// Health
app.get('/api/health', (req, res) => {
  res.json({ success: true, status: 'OK', timestamp: new Date().toISOString(), version: '1.0.0', txt_files_dir: TXT_FILES_DIR });
});

// Info
app.get('/api/info', authenticateToken, (req, res) => {
  res.json({
    success: true,
    user: req.user,
    server: { node: process.version, uptime: process.uptime(), memory: process.memoryUsage(), txt_files_directory: TXT_FILES_DIR }
  });
});

/* =========================================================
 *                     ERROR HANDLING
 * =======================================================*/
app.use((error, req, res, next) => {
  console.error('Errore non gestito:', error);
  res.status(500).json({ error: 'Errore interno del server' });
});

app.use('*', (req, res) => {
  console.log('❌ Route non trovata:', req.originalUrl);
  res.status(404).json({ error: 'Endpoint non trovato' });
});

/* =========================================================
 *                         START
 * =======================================================*/
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server in esecuzione su 0.0.0.0:${PORT}`);
  console.log(`📱 Accesso LAN: http://192.168.60.142:${PORT}`);
  console.log(`🔐 JWT Secret configurato: ${!!process.env.JWT_SECRET}`);
  console.log(`📊 Google Sheets ID: ${GOOGLE_SHEET_ID}`);
  console.log(`🤖 Google Service Account configurato: ${!!GOOGLE_SERVICE_ACCOUNT_EMAIL}`);
  console.log(`📁 Cartella file TXT: ${TXT_FILES_DIR}`);
});

module.exports = app;
