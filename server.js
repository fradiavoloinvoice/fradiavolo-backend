// server.js
const express = require('express');
const archiver = require('archiver');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { GoogleSpreadsheet } = require('google-spreadsheet');
const { JWT } = require('google-auth-library');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
const validator = require('validator');
const path = require('path');
const fs = require('fs').promises;
require('dotenv').config();

const negozi = require('./data/negozi.json');

const app = express();
const PORT = process.env.PORT || 3001;

const localServiceAccountPath = path.join(__dirname, 'credentials', 'google-service-account.local.json');
let localServiceAccount;
try { localServiceAccount = require(localServiceAccountPath); } catch (e) { if (e.code !== 'MODULE_NOT_FOUND') console.warn(e.message); }

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

const TXT_FILES_DIR = path.join(__dirname, 'generated_txt_files');
const ensureTxtDir = async () => { try { await fs.access(TXT_FILES_DIR); } catch { await fs.mkdir(TXT_FILES_DIR, { recursive: true }); } };
ensureTxtDir();

app.use(helmet());
app.use(cors({ origin: true, credentials: true }));
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || localServiceAccount?.client_email;
const rawGooglePrivateKey = process.env.GOOGLE_PRIVATE_KEY || localServiceAccount?.private_key;
const GOOGLE_PRIVATE_KEY = rawGooglePrivateKey ? rawGooglePrivateKey.replace(/\\n/g, '\n') : undefined;

const serviceAccountAuth = new JWT({
  email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
  key: GOOGLE_PRIVATE_KEY,
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

const getGoogleDoc = async () => { const doc = new GoogleSpreadsheet(GOOGLE_SHEET_ID, serviceAccountAuth); await doc.loadInfo(); return doc; };
const getGoogleSheet = async (sheetName = null) => {
  const doc = await getGoogleDoc();
  if (sheetName) {
    const sheet = doc.sheetsByTitle[sheetName];
    if (!sheet) throw new Error(`Foglio "${sheetName}" non trovato`);
    return sheet;
  }
  return doc.sheetsByIndex[0];
};

/** =======================
 *  SHEETS LOADERS (agg.)
 *  ======================= */
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
    // ⬇️ colonna O (errore di conversione)
    item_noconv: row.get('item_noconv') || ''
  }));
  const unique = data.filter((inv, i, self) => i === self.findIndex(x => x.id === inv.id));
  return unique;
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

/** ==========================
 *  GENERAZIONE TXT (aggiornata)
 *  ========================== */
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
  return { fileName, filePath, size: contenutoTxt.length, hasErrors, noteErrori: noteErrori || null, item_noconv: itemNoConv || null };
};

/** ======================
 *  AUTH & UTILS (invariato)
 *  ====================== */
const users = [/* ... (tuo array utenti invariato) ... */];

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

/** ===========================
 *  UPDATE ROW (passa item_noconv)
 *  =========================== */
const updateSheetRow = async (id, updates) => {
  const sheet = await getGoogleSheet();
  const rows = await sheet.getRows();
  const row = rows.find(r => r.get('id') === id.toString());
  if (!row) throw new Error('Fattura non trovata');

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
    // ⬇️ includo colonna O
    item_noconv: row.get('item_noconv') || ''
  };

  Object.keys(updates).forEach(k => row.set(k, updates[k]));
  await row.save();

  if (updates.stato === 'consegnato') {
    try { await generateTxtFile(invoiceDataForTxt); } catch (e) { console.error(e); }
  }
  return true;
};

/** ===========================
 *  ROUTES TXT FILES (aggiornate)
 *  =========================== */
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
    // Ordina per creato desc
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

app.get('/api/txt-files/:filename/content', authenticateToken, async (req, res) => {
  try {
    let { filename } = req.params;
    if (!filename.endsWith('.txt') || filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: 'Nome file non valido' });
    }

    // ricavo numero documento dal filename: NUMERO_YYYY-MM-DD_...
    const numeroDoc = filename.split('_')[0];

    const allInvoices = await loadAllSheetData();
    const related = allInvoices.find(inv => inv.numero === numeroDoc);

    // error flags da sheet: note o item_noconv
    const note_errori = related?.note || '';
    const item_noconv = related?.item_noconv || '';
    const shouldHaveErrorSuffix = (!!note_errori && note_errori.trim() !== '') || (!!item_noconv && item_noconv.trim() !== '');

    const currentlyHasSuffix = filename.includes('_ERRORI.txt');
    let renamedTo = null;

    // se deve avere suffix ma non ce l'ha -> rinomina fisicamente
    if (shouldHaveErrorSuffix && !currentlyHasSuffix) {
      const newFilename = filename.replace(/\.txt$/i, '_ERRORI.txt');
      try {
        const oldPath = path.join(TXT_FILES_DIR, filename);
        const newPath = path.join(TXT_FILES_DIR, newFilename);
        await fs.access(oldPath); // se manca, salta
        await fs.rename(oldPath, newPath);
        filename = newFilename; // aggiorno per lettura e risposta
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
        note_errori: note_errori || undefined,
        item_noconv: item_noconv || undefined,
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

// Scarica come ZIP tutti i file di una data
app.get('/api/txt-files/download-by-date/:date', authenticateToken, async (req, res) => {
  try {
    const { date } = req.params;
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'Formato data non valido. Usa YYYY-MM-DD' });
    }

    const allFiles = await fs.readdir(TXT_FILES_DIR);
    const txtFiles = allFiles.filter(file => file.endsWith('.txt') && !file.includes('.backup'));

    const filesForDate = txtFiles.filter(filename => {
      const m = filename.match(/(\d{4}-\d{2}-\d{2})/);
      return m && m[1] === date;
    });

    if (filesForDate.length === 0) {
      return res.status(404).json({ error: `Nessun file TXT trovato per la data ${date}` });
    }

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
    console.error('❌ Errore creazione ZIP:', error);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Impossibile creare l\'archivio ZIP: ' + error.message });
    }
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
      if (!m) {
        console.warn('⚠️ Nome file senza data riconoscibile:', filename);
        continue;
      }
      const datePart = m[1];
      if (!filesByDate[datePart]) filesByDate[datePart] = [];

      const filePath = path.join(TXT_FILES_DIR, filename);
      const stats = await fs.stat(filePath);
      filesByDate[datePart].push({
        name: filename,
        size: stats.size,
        created: stats.birthtime,
        modified: stats.mtime
      });
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
    console.error('❌ Errore calcolo statistiche:', error);
    res.status(500).json({ error: 'Impossibile calcolare le statistiche: ' + error.message });
  }
});

// Health check
app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    status: 'OK',
    timestamp: new Date().toISOString(),
    version: '1.0.0',
    txt_files_dir: TXT_FILES_DIR
  });
});

// Info sistema
app.get('/api/info', authenticateToken, (req, res) => {
  res.json({
    success: true,
    user: req.user,
    server: {
      node: process.version,
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      txt_files_directory: TXT_FILES_DIR
    }
  });
});

// ==========================================
// ERROR HANDLING
// ==========================================
app.use((error, req, res, next) => {
  console.error('Errore non gestito:', error);
  res.status(500).json({ error: 'Errore interno del server' });
});

app.use('*', (req, res) => {
  console.log('❌ Route non trovata:', req.originalUrl);
  res.status(404).json({ error: 'Endpoint non trovato' });
});

// ==========================================
// START SERVER
// ==========================================
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server in esecuzione su 0.0.0.0:${PORT}`);
  console.log(`📱 Accesso mobile: http://192.168.60.142:${PORT}`);
  console.log(`🔐 JWT Secret configurato: ${!!process.env.JWT_SECRET}`);
  console.log(`📊 Google Sheets ID: ${GOOGLE_SHEET_ID}`);
  console.log(`🤖 Google Service Account configurato: ${!!GOOGLE_SERVICE_ACCOUNT_EMAIL}`);
  console.log(`📁 Cartella file TXT: ${TXT_FILES_DIR}`);
});

module.exports = app;
