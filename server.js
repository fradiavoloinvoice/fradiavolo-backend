// server.js - AGGIORNAMENTO PER GENERAZIONE FILE TXT (VERSIONE CORRETTA)
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
const fs = require('fs').promises; // 📁 per la gestione file
require('dotenv').config();

// Importa i dati dei negozi per ottenere i codici
const negoziData = require('../frontend/src/data/negozi.json'); // 📊

const app = express();
const PORT = process.env.PORT || 3001;
const localServiceAccountPath = path.join(__dirname, 'credentials', 'google-service-account.local.json');
let localServiceAccount;

try {
  localServiceAccount = require(localServiceAccountPath);
} catch (error) {
  if (error.code !== 'MODULE_NOT_FOUND') {
    console.warn('⚠️ Impossibile leggere le credenziali locali:', error.message);
  }
}

const hasGoogleEmailEnv = Boolean(process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL);
const hasGoogleEmailLocal = Boolean(localServiceAccount?.client_email);
const hasGoogleKeyEnv = Boolean(process.env.GOOGLE_PRIVATE_KEY);
const hasGoogleKeyLocal = Boolean(localServiceAccount?.private_key);
const googleEmailSource = hasGoogleEmailEnv ? 'env' : hasGoogleEmailLocal ? 'local file' : 'missing';
const googleKeySource = hasGoogleKeyEnv ? 'env' : hasGoogleKeyLocal ? 'local file' : 'missing';
const GOOGLE_SHEET_ID = process.env.GOOGLE_SHEET_ID;

// ==========================================
// VERIFICA CONFIGURAZIONE STARTUP
// ==========================================
console.log('🔍 VERIFICA CONFIGURAZIONE STARTUP:');
console.log('📊 PORT:', PORT);
console.log('🔐 JWT_SECRET configurato:', !!process.env.JWT_SECRET);
console.log('📊 GOOGLE_SHEET_ID:', GOOGLE_SHEET_ID ? 'CONFIGURATO' : 'MANCANTE');
console.log('🤖 GOOGLE_SERVICE_ACCOUNT_EMAIL:', googleEmailSource === 'missing' ? 'MANCANTE' : `CONFIGURATO (${googleEmailSource})`);
console.log('🔑 GOOGLE_PRIVATE_KEY:', googleKeySource === 'missing' ? 'MANCANTE' : `DISPONIBILE (${googleKeySource})`);

// Crea cartella per i file TXT se non esiste
const TXT_FILES_DIR = path.join(__dirname, 'generated_txt_files');

const ensureTxtDir = async () => {
  try {
    await fs.access(TXT_FILES_DIR);
  } catch {
    console.log('📁 Creando cartella per file TXT:', TXT_FILES_DIR);
    await fs.mkdir(TXT_FILES_DIR, { recursive: true });
  }
};

// Inizializza la cartella all'avvio
ensureTxtDir()
  .then(() => console.log('📁 Cartella file TXT pronta:', TXT_FILES_DIR))
  .catch(error => console.error('❌ Errore creazione cartella TXT:', error));

// ==========================================
// MIDDLEWARE DI SICUREZZA
// ==========================================
app.use(helmet());
app.use(cors({ origin: true, credentials: true }));

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: 'Troppe richieste da questo IP'
});
app.use(limiter);

const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 5,
  message: 'Troppi tentativi di login. Riprova tra 15 minuti.'
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

// ==========================================
// CONFIGURAZIONE GOOGLE SHEETS
// ==========================================
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || localServiceAccount?.client_email;
const rawGooglePrivateKey = process.env.GOOGLE_PRIVATE_KEY || localServiceAccount?.private_key;
const GOOGLE_PRIVATE_KEY = rawGooglePrivateKey ? rawGooglePrivateKey.replace(/\\n/g, '\n') : undefined;

if (!GOOGLE_SERVICE_ACCOUNT_EMAIL || !GOOGLE_PRIVATE_KEY) {
  console.warn("⚠️ Credenziali Google incomplete: configura le variabili d'ambiente o il file locale.");
}

const serviceAccountAuth = new JWT({
  email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
  key: GOOGLE_PRIVATE_KEY,
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});

// Helpers: doc e sheet
const getGoogleDoc = async () => {
  const doc = new GoogleSpreadsheet(GOOGLE_SHEET_ID, serviceAccountAuth);
  await doc.loadInfo();
  return doc;
};

const getGoogleSheet = async (sheetName = null) => {
  try {
    const doc = await getGoogleDoc();
    if (sheetName) {
      const sheet = doc.sheetsByTitle[sheetName];
      if (!sheet) throw new Error(`Foglio "${sheetName}" non trovato`);
      return sheet;
    }
    return doc.sheetsByIndex[0];
  } catch (error) {
    console.error('Errore connessione Google Sheets:', error);
    throw new Error('Impossibile connettersi a Google Sheets');
  }
};

// FUNZIONE AGGIORNATA PER GENERARE FILE TXT CON GESTIONE ERRORI

const generateTxtFile = async (invoiceData) => {
  try {
    console.log('📄 Generando file TXT per fattura:', invoiceData.id);

    const numeroDocumento = invoiceData.numero;        // Colonna B
    const dataConsegna = invoiceData.data_consegna;  // Colonna E
    const nomeFornitore = invoiceData.fornitore;       // Colonna C
    const puntoVendita = invoiceData.punto_vendita;
    const contenutoTxt = invoiceData.txt || '';        // Colonna L
    const noteErrori = invoiceData.note || '';         // Note errori dalla conferma

    const negozio = negoziData.find(n => n.nome === puntoVendita);
    const codicePV = negozio?.codice || 'UNKNOWN';

    if (!numeroDocumento) throw new Error('Numero documento mancante');
    if (!dataConsegna) throw new Error('Data di consegna mancante');
    if (!nomeFornitore) throw new Error('Nome fornitore mancante');

    if (!contenutoTxt || contenutoTxt.trim() === '') {
      console.log('⚠️ Contenuto TXT vuoto, salto la generazione del file');
      return null;
    }

    const cleanForFilename = (str) =>
      String(str)
        .replace(/[\/\\:*?"<>|]/g, '_')
        .replace(/\s+/g, '_')
        .replace(/_{2,}/g, '_')
        .trim();

    const dataFormatted = dataConsegna; // ISO (YYYY-MM-DD)
    const numeroDocPulito = cleanForFilename(numeroDocumento);
    const nomeFornitorePulito = cleanForFilename(nomeFornitore);
    const codicePVPulito = cleanForFilename(codicePV);

    // Determina se ci sono errori
    const hasErrors = noteErrori && noteErrori.trim() !== '';
    const errorSuffix = hasErrors ? '_ERRORI' : '';

    const fileName = `${numeroDocPulito}_${dataFormatted}_${nomeFornitorePulito}_${codicePVPulito}${errorSuffix}.txt`;
    const filePath = path.join(TXT_FILES_DIR, fileName);

    // Il file deve contenere solo il contenuto originale pulito
    const finalContent = contenutoTxt;

    await fs.writeFile(filePath, finalContent, 'utf8');
    
    const logMessage = hasErrors 
      ? `⚠️ File TXT CON ERRORI generato: ${fileName}` 
      : `✅ File TXT generato con successo: ${fileName}`;
    console.log(logMessage);

    return { 
      fileName, 
      filePath, 
      size: finalContent.length,
      hasErrors,
      noteErrori: hasErrors ? noteErrori : null
    };
  } catch (error) {
    console.error('❌ Errore generazione file TXT:', error);
    throw error;
  }
};
// ==========================================
// DATABASE UTENTI (mock in chiaro per POC)
// ==========================================
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

console.log('👥 Utenti disponibili:', users.length);
console.log('🏢 Punti vendita configurati:', [...new Set(users.map(u => u.puntoVendita))].length);

// ==========================================
// MIDDLEWARE DI AUTENTICAZIONE JWT
// ==========================================
const authenticateToken = (req, res, next) => {
  const token = (req.headers['authorization'] || '').split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Token di accesso richiesto' });

  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ error: 'Token non valido' });
    req.user = user;
    next();
  });
};

// Admin only
const requireAdmin = (req, res, next) => {
  console.log('🔒 Verifica permessi admin per:', req.user.email, 'Role:', req.user.role);
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Accesso riservato agli amministratori' });
  next();
};

// ==========================================
// FUNZIONI DI VALIDAZIONE
// ==========================================
const validateEmail = (email) =>
  validator.isEmail(email) &&
  (email.includes('@fradiavolopizzeria.com') || email.includes('@azienda.it'));

const validateDate = (dateString) =>
  validator.isDate(dateString) && new Date(dateString) <= new Date();

const sanitizeInput = (input) => validator.escape(String(input ?? '').trim());

// ==========================================
// FUNZIONI GOOGLE SHEETS - FATTURE
// ==========================================
const loadAllSheetData = async () => {
  try {
    console.log('📊 Admin: Caricamento dati globali da Google Sheets');
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
      testo_ddt: row.get('testo_ddt') || ''
    }));

    // Rimuovi duplicati
    const uniqueData = data.filter((invoice, index, self) =>
      index === self.findIndex(i => i.id === invoice.id)
    );

    return uniqueData;
  } catch (error) {
    console.error('❌ Admin: Errore caricamento dati globali:', error);
    throw error;
  }
};

// Caricamento filtrato per punto vendita (operator)
const loadSheetData = async (puntoVendita) => {
  try {
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
      testo_ddt: row.get('testo_ddt') || ''
    }));

    if (puntoVendita) {
      data = data.filter(r => r.punto_vendita === puntoVendita);
    }
    return data;
  } catch (error) {
    console.error('❌ Errore loadSheetData:', error);
    throw error;
  }
};

const loadAllMovimentazioniData = async () => {
  try {
    console.log('📦 Admin: Caricamento movimentazioni globali');
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
  } catch (error) {
    console.error('❌ Admin: Errore caricamento movimentazioni globali:', error);
    throw error;
  }
};

// ==========================================
// MOVIMENTAZIONI: CARICAMENTO PER PUNTO VENDITA
// ==========================================
const loadMovimentazioniFromSheet = async (puntoVendita) => {
  try {
    console.log('📦 Caricamento movimentazioni per:', puntoVendita);

    let sheet;
    try {
      sheet = await getGoogleSheet('Movimentazioni');
    } catch (sheetError) {
      console.error('❌ Errore accesso foglio Movimentazioni:', sheetError.message);
      // crea foglio
      const doc = await getGoogleDoc();
      sheet = await doc.addSheet({
        title: 'Movimentazioni',
        headerValues: [
          'id', 'data_movimento', 'timestamp', 'origine', 'codice_origine',
          'prodotto', 'quantita', 'unita_misura', 'destinazione', 'codice_destinazione',
          'stato', 'txt_content', 'txt_filename', 'creato_da'
        ]
      });
    }

    const rows = await sheet.getRows();
    let data = rows.map(row => ({
      id: row.get('id') || 'N/A',
      data_movimento: row.get('data_movimento') || 'N/A',
      timestamp: row.get('timestamp') || 'N/A',
      origine: row.get('origine') || 'N/A',
      codice_origine: row.get('codice_origine') || '',
      prodotto: row.get('prodotto') || 'N/A',
      quantita: row.get('quantita') || '0',
      unita_misura: row.get('unita_misura') || '',
      destinazione: row.get('destinazione') || 'N/A',
      codice_destinazione: row.get('codice_destinazione') || '',
      stato: row.get('stato') || 'registrato',
      txt_content: row.get('txt_content') || '',
      txt_filename: row.get('txt_filename') || '',
      creato_da: row.get('creato_da') || ''
    }));

    if (puntoVendita && puntoVendita !== 'ADMIN_GLOBAL') {
      data = data.filter(item => item.origine === puntoVendita);
    }

    data.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    return data;
  } catch (error) {
    console.error('❌ Errore caricamento movimentazioni:', error);
    return [];
  }
};
// ==========================================
// ==========================================
// FUNZIONE PER CARICARE PRODOTTI DA GOOGLE SHEETS (senza tagli)
// ==========================================
const loadProdottiFromSheet = async () => {
  try {
    console.log('📦 Caricando prodotti da database Google Sheets...');

    // ID del foglio prodotti (quello che hai condiviso col service account)
    const PRODOTTI_SHEET_ID = '1CJhd14F8qV8nS0-SK2ENSNSkWaE21KotK2ArBjJETfk';

    // Connessione al doc prodotti con le stesse credenziali SA che usi altrove
    const prodottiDoc = new GoogleSpreadsheet(PRODOTTI_SHEET_ID, serviceAccountAuth);
    await prodottiDoc.loadInfo();

    // Usa il tab “PRODOTTI” se esiste, altrimenti il primo
    const sheet = prodottiDoc.sheetsByTitle?.['PRODOTTI'] || prodottiDoc.sheetsByIndex[0];
    if (!sheet) throw new Error('Tab prodotti non trovato');

    const rows = await sheet.getRows(); // ⚠️ nessun limit: legge tutto
    console.log('📊 Righe prodotti caricate:', rows.length);

    // Mapping robusto dei campi (adatta ai nomi colonna del tuo foglio)
    const norm = (v) => (v ?? '').toString().trim();
    const prodotti = rows.map(r => {
      const nome   = norm(r.get('Nome') || r.get('DESCRIZIONE') || r.get('Descrizione') || r.get('nome'));
      const codice = norm(r.get('Cod.mago') || r.get('CODICE') || r.get('Codice') || r.get('SKU') || r.get('codice'));
      const uom    = norm(r.get('UMB') || r.get('UM') || r.get('UOM') || r.get('unita_misura'));
      const onoff  = norm(r.get('On.Off') ?? r.get('On') ?? r.get('Attivo') ?? r.get('active'));

      // campi aggiuntivi utili per la ricerca lato client
      const brand      = norm(r.get('Marca') || r.get('Brand') || r.get('brand'));
      const pack       = norm(r.get('Confezione') || r.get('Pack') || r.get('Formato'));
      const materiale  = norm(r.get('Materiale') || r.get('Imballo') || r.get('Package'));

      return {
        nome,
        codice,
        unitaMisura: uom,
        onOff: onoff,
        brand,
        pack,
        materiale
      };
    });

    // NON filtriamo e NON ordiniamo qui: lo farà la rotta a seconda dei query param
    return prodotti.filter(p => p.nome); // scarta eventuali righe vuote
  } catch (error) {
    console.error('❌ Errore caricamento prodotti da Google Sheets:', error);

    // Fallback: prova a caricare un JSON locale se esiste
    try {
      const prodottiData = require('../frontend/src/data/prodotti.json');
      return Array.isArray(prodottiData) ? prodottiData : [];
    } catch {
      return [];
    }
  }
};


// ==========================================
// UPDATE RIGA FATTURA + GENERAZIONE TXT SE CONSEGNATO
// ==========================================
const updateSheetRow = async (id, updates) => {
  try {
    console.log('🔄 updateSheetRow chiamata con:', { id, updates });

    const sheet = await getGoogleSheet();
    const rows = await sheet.getRows();

    const row = rows.find(r => r.get('id') === id.toString());
    if (!row) {
      console.error('❌ Riga non trovata per ID:', id);
      throw new Error('Fattura non trovata');
    }

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
      note: updates.note || row.get('note') || ''
    };

    // Applica aggiornamenti
    Object.keys(updates).forEach(key => row.set(key, updates[key]));
    await row.save();

    // Genera file TXT se lo stato diventa "consegnato"
    if (updates.stato === 'consegnato') {
      try {
        const txtResult = await generateTxtFile(invoiceDataForTxt);
        if (txtResult) console.log('✅ File TXT generato:', txtResult.fileName);
        else console.log('ℹ️ File TXT non generato (contenuto vuoto)');
      } catch (txtError) {
        console.error('❌ Errore generazione file TXT:', txtError);
      }
    }

    return true;
  } catch (error) {
    console.error('❌ Errore updateSheetRow:', error);
    throw error;
  }
};

// ==========================================
// GENERA FATTURA DA MOVIMENTAZIONE (UNA SOLA DEFINIZIONE)
// ==========================================
const generateInvoiceFromMovimentazione = async (movimentazioneData) => {
  try {
    console.log('📄 Generando fattura automatica da movimentazione:', movimentazioneData);

    const sheet = await getGoogleSheet();

    const rows = await sheet.getRows();
    const existingNumbers = rows
      .map(row => row.get('numero'))
      .filter(num => num && num.startsWith('MOV'))
      .map(num => parseInt(num.replace('MOV', ''), 10))
      .filter(num => !isNaN(num));

    const nextNumber = existingNumbers.length > 0 ? Math.max(...existingNumbers) + 1 : 1;
    const numeroFattura = `MOV${String(nextNumber).padStart(4, '0')}`;

    const timestamp = Date.now();
    const uniqueId = `mov_${timestamp}_${nextNumber}`;
  const txtContent = movimentazioneData.txt_content || '';
    const fatturaData = {
      // id: uniqueId, //
      numero: numeroFattura,
      fornitore: movimentazioneData.origine,                // uscita
      data_emissione: movimentazioneData.data_movimento,
      data_consegna: '',
      stato: 'pending',
      punto_vendita: movimentazioneData.destinazione,       // entrata
      confermato_da: '',
      //pdf_link: '#', //
      // importo_totale: '0.00', //
     // note: `Fattura automatica da movimentazione: ${movimentazioneData.prodotto} (${movimentazioneData.quantita} ${movimentazioneData.unita_misura})`,
     txt: txtContent,
      codice_fornitore: movimentazioneData.codice_origine || 'TRANSFER'
    };

    await sheet.addRow(fatturaData);
    console.log('✅ Fattura automatica creata:', numeroFattura);

    return { success: true, numeroFattura, fatturaId: uniqueId };
  } catch (error) {
    console.error('❌ Errore generazione fattura da movimentazione:', error);
    return { success: false, error: error.message };
  }
};

// ==========================================
// ROUTES - AUTENTICAZIONE
// ==========================================
app.post('/api/auth/login', loginLimiter, async (req, res) => {
  console.log('🔄 POST /api/auth/login ricevuta');
  try {
    const { email, password } = req.body;
    if (!email || !password) return res.status(400).json({ error: 'Email e password richiesti' });

    if (!validateEmail(email)) {
      return res.status(400).json({ error: 'Email non valida - deve contenere @fradiavolopizzeria.com' });
    }

    const user = users.find(u => u.email === email);
    if (!user) return res.status(401).json({ error: 'Credenziali non valide' });

    // POC: confronto in chiaro
    const isValidPassword = password === user.password;
    if (!isValidPassword) return res.status(401).json({ error: 'Credenziali non valide' });

    if (!process.env.JWT_SECRET) {
      return res.status(500).json({ error: 'Configurazione server non valida' });
    }

    const tokenPayload = { userId: user.id, email: user.email, puntoVendita: user.puntoVendita, role: user.role };
    const token = jwt.sign(tokenPayload, process.env.JWT_SECRET, { expiresIn: '8h' });

    res.json({
      success: true,
      token,
      user: { id: user.id, name: user.name, email: user.email, puntoVendita: user.puntoVendita, role: user.role }
    });
  } catch (error) {
    console.error('❌ Errore durante login:', error);
    res.status(500).json({ error: 'Errore interno del server: ' + error.message });
  }
});

app.get('/api/auth/verify', authenticateToken, (req, res) => {
  console.log('🔄 GET /api/auth/verify ricevuta');
  try {
    const user = users.find(u => u.id === req.user.userId);
    if (!user) return res.status(401).json({ error: 'Utente non trovato' });

    res.json({
      success: true,
      user: { id: user.id, name: user.name, email: user.email, puntoVendita: user.puntoVendita, role: user.role }
    });
  } catch (error) {
    console.error('❌ Errore verifica token:', error);
    res.status(500).json({ error: 'Errore interno del server' });
  }
});

app.post('/api/auth/logout', authenticateToken, (req, res) => {
  console.log('🔄 POST /api/auth/logout ricevuta');
  res.json({ success: true, message: 'Logout effettuato' });
});

// ==========================================
// ROUTES - FATTURE
// ==========================================
app.get('/api/invoices', authenticateToken, async (req, res) => {
  console.log('🔄 GET /api/invoices ricevuta');
  try {
    const puntoVendita = req.user.role === 'admin' ? null : req.user.puntoVendita;
    const data = req.user.role === 'admin'
      ? await loadAllSheetData()
      : await loadSheetData(puntoVendita);
    res.json({ success: true, data });
  } catch (error) {
    console.error('❌ Errore caricamento fatture:', error);
    res.status(500).json({ error: 'Impossibile caricare le fatture' });
  }
});

app.post('/api/invoices/:id/confirm', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { data_consegna, note_errori, confermato_da_email } = req.body;

    if (!id || !data_consegna) {
      return res.status(400).json({ error: 'ID fattura e data consegna richiesti' });
    }
    if (!validateDate(data_consegna)) return res.status(400).json({ error: 'Data non valida' });

    let confermatoDa = req.user.email;
    if (req.user.role === 'admin' && confermato_da_email && validateEmail(confermato_da_email)) {
      confermatoDa = sanitizeInput(confermato_da_email);
    }

    const updates = {
      stato: 'consegnato',
      data_consegna: sanitizeInput(data_consegna),
      confermato_da: confermatoDa
    };
    if (note_errori && note_errori.trim()) updates.note = sanitizeInput(note_errori.trim());

    await updateSheetRow(id, updates);
    res.json({ success: true, message: 'Consegna confermata' });
  } catch (error) {
    console.error('Errore conferma:', error);
    res.status(500).json({ error: 'Impossibile confermare la consegna' });
  }
});

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
    res.json({ success: true, message: 'Fattura aggiornata con successo', updated_fields: Object.keys(updates) });
  } catch (error) {
    console.error('❌ Errore aggiornamento fattura:', error);
    res.status(500).json({ error: 'Impossibile aggiornare la fattura: ' + error.message });
  }
});

// ==========================================
// ROUTES - MOVIMENTAZIONI
// ==========================================
const saveMovimentazioniToSheet = async (movimenti, origine) => {
  try {
    console.log('📦 Salvando movimentazioni su Google Sheets...');

    let sheet;
    try {
      sheet = await getGoogleSheet('Movimentazioni');
    } catch {
      console.error('❌ Foglio "Movimentazioni" non trovato, creazione in corso...');
      const doc = await getGoogleDoc();
      sheet = await doc.addSheet({
        title: 'Movimentazioni',
        headerValues: [
          'id', 'data_movimento', 'timestamp', 'origine', 'codice_origine',
          'prodotto', 'quantita', 'unita_misura', 'destinazione', 'codice_destinazione',
          'stato', 'txt_content', 'txt_filename', 'creato_da'
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

    // Genera fatture automatiche
    const fattureGenerate = [];
    for (let i = 0; i < righe.length; i++) {
      const fatturaResult = await generateInvoiceFromMovimentazione(righe[i]);
      fattureGenerate.push(fatturaResult);
    }

    return {
      success: true,
      righe_inserite: righe.length,
      fatture_generate: fattureGenerate.filter(f => f.success).length,
      fatture_errori: fattureGenerate.filter(f => !f.success).length,
      dettagli_fatture: fattureGenerate
    };
  } catch (error) {
    console.error('❌ Errore salvataggio movimentazioni:', error);
    throw error;
  }
};

app.get('/api/movimentazioni', authenticateToken, async (req, res) => {
  console.log('🔄 GET /api/movimentazioni ricevuta');
  try {
    const data = await loadMovimentazioniFromSheet(req.user.puntoVendita);
    res.json({ success: true, data });
  } catch (error) {
    console.error('❌ Errore caricamento movimentazioni:', error);
    res.status(500).json({ error: 'Impossibile caricare le movimentazioni' });
  }
});

app.post('/api/movimentazioni', authenticateToken, async (req, res) => {
  console.log('🔄 POST /api/movimentazioni ricevuta');
  try {
    const { movimenti, origine, creato_da_email } = req.body;

    if (!movimenti || !Array.isArray(movimenti) || movimenti.length === 0) {
      return res.status(400).json({ error: 'Lista movimenti richiesta' });
    }
    if (!origine || origine.trim() === '') {
      return res.status(400).json({ error: 'Punto vendita di origine richiesto' });
    }
    if (req.user.puntoVendita !== origine && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Non autorizzato per questo punto vendita' });
    }

    // Se admin e creato_da_email fornito, trova utente e aggiorna origine/creato_da
    let customOrigin = origine;
    let customCreatoDa = req.user.email;
    if (req.user.role === 'admin' && creato_da_email && validateEmail(creato_da_email)) {
      const userObj = users.find(u => u.email === creato_da_email);
      if (userObj) {
        customOrigin = userObj.puntoVendita;
        customCreatoDa = userObj.email;
      }
    }

    // Elabora + salva TXT locali
    for (let i = 0; i < movimenti.length; i++) {
      const movimento = movimenti[i];

      if (!movimento.prodotto || movimento.prodotto.trim() === '') {
        return res.status(400).json({ error: `Prodotto richiesto per movimento ${i + 1}` });
      }
      if (!movimento.quantita || isNaN(movimento.quantita) || movimento.quantita <= 0) {
        return res.status(400).json({ error: `Quantità valida richiesta per movimento ${i + 1}` });
      }
      if (!movimento.destinazione || movimento.destinazione.trim() === '') {
        return res.status(400).json({ error: `Destinazione richiesta per movimento ${i + 1}` });
      }

      // Sanitizza
      movimento.prodotto = sanitizeInput(movimento.prodotto);
      movimento.quantita = parseFloat(movimento.quantita);
      movimento.unita_misura = sanitizeInput(movimento.unita_misura || '');
      movimento.destinazione = sanitizeInput(movimento.destinazione);
      movimento.txt_content = movimento.txt_content || '';
      movimento.creato_da = customCreatoDa;
      movimento.origine = customOrigin;
    }

    const result = await saveMovimentazioniToSheet(movimenti, sanitizeInput(customOrigin));

    let successMessage = `✅ ${result.righe_inserite} movimenti registrati con successo`;
    if (result.fatture_generate > 0) {
      successMessage += ` - ${result.fatture_generate} fattura/e automatica/e create nella sezione "Da Confermare"`;
    }
    if (result.fatture_errori > 0) {
      successMessage += ` - ${result.fatture_errori} errore/i nella generazione fatture`;
    }
    if (result.dettagli_fatture?.length) {
      const ok = result.dettagli_fatture.filter(f => f.success).map(f => f.numeroFattura);
      if (ok.length) successMessage += ` - Fatture: ${ok.join(', ')}`;
    }

    res.json({
      success: true,
      message: successMessage,
      data: result
    });
  } catch (error) {
    console.error('❌ Errore POST /api/movimentazioni:', error);
    res.status(500).json({ error: 'Impossibile salvare le movimentazioni' });
  }
}); // <-- ✅ CHIUSURA ENDPOINT POST /api/movimentazioni

// Endpoint per ottenere la lista prodotti da Google Sheets (con filtro/paginazione opzionali)
app.get('/api/prodotti', authenticateToken, async (req, res) => {
  console.log('🔄 GET /api/prodotti ricevuta');
  console.log('👤 Richiesta da utente:', req.user.email);

  try {
    const { active, page, per_page } = req.query;

    // 1) Carica TUTTI i prodotti
    const all = await loadProdottiFromSheet();

    // 2) Filtro opzionale "solo attivi"
    const onlyActive = active === '1';
    let filtered = onlyActive
      ? all.filter(p => ['1', 1, true, 'TRUE', 'true'].includes(p.onOff))
      : all;

    // 3) Ordinamento alfabetico per nome (utile per la ricerca client)
    filtered.sort((a, b) => (a.nome || '').localeCompare(b.nome || '', 'it', { sensitivity: 'base' }));

    // 4) Paginazione opzionale (?page=1&per_page=1000)
    const total = filtered.length;
    let data = filtered;
    let nextPage = null;

    const perPageNum = Math.max(parseInt(per_page || '0', 10), 0); // 0 = no paginazione
    const pageNum = Math.max(parseInt(page || '1', 10), 1);

    if (perPageNum > 0) {
      const start = (pageNum - 1) * perPageNum;
      const end = start + perPageNum;
      data = filtered.slice(start, end);
      if (end < total) nextPage = pageNum + 1;

      // Intestazioni utili
      res.set('X-Total-Count', String(total));
      if (nextPage) {
        const base = req.originalUrl.split('?')[0];
        res.set('Link', `<${base}?page=${nextPage}&per_page=${perPageNum}${onlyActive ? '&active=1' : ''}>; rel="next"`);
      }
    }

    // 5) Risposta
    res.json({
      success: true,
      sheet: 'PRODOTTI',
      total,
      returned: data.length,
      nextPage,
      data
    });
  } catch (error) {
    console.error('❌ Errore caricamento prodotti:', error);
    res.status(500).json({
      error: 'Impossibile caricare la lista prodotti',
      details: error.message
    });
  }
});
// ==========================================
// ROUTES - ADMIN
// ==========================================
app.get('/api/admin/dashboard', authenticateToken, requireAdmin, async (req, res) => {
  console.log('🔄 GET /api/admin/dashboard ricevuta');
  try {
    const [invoices, movimentazioni] = await Promise.all([
      loadAllSheetData(),
      loadAllMovimentazioniData()
    ]);

    const stats = {
      invoices: {
        total: invoices.length,
        consegnate: invoices.filter(inv => inv.stato === 'consegnato').length,
        pending: invoices.filter(inv => inv.stato === 'in_consegna').length,
        byStore: {},
        byStatus: {},
        recentActivity: invoices
          .filter(inv => inv.data_consegna)
          .sort((a, b) => new Date(b.data_consegna) - new Date(a.data_consegna))
          .slice(0, 10)
      },
      movimentazioni: {
        total: movimentazioni.length,
        thisMonth: movimentazioni.filter(mov => {
          const movDate = new Date(mov.timestamp);
          const now = new Date();
          return movDate.getMonth() === now.getMonth() && movDate.getFullYear() === now.getFullYear();
        }).length,
        byStore: {},
        recentActivity: movimentazioni.slice(0, 10)
      },
      activeStores: [...new Set([
        ...invoices.map(inv => inv.punto_vendita),
        ...movimentazioni.map(mov => mov.origine)
      ])].filter(Boolean),
      dateRange: {
        firstInvoice: invoices.reduce((earliest, inv) => {
          const date = new Date(inv.data_emissione);
          return !earliest || date < earliest ? date : earliest;
        }, null),
        lastActivity: new Date()
      }
    };

    invoices.forEach(inv => {
      if (inv.punto_vendita) {
        if (!stats.invoices.byStore[inv.punto_vendita]) {
          stats.invoices.byStore[inv.punto_vendita] = { total: 0, consegnate: 0, pending: 0 };
        }
        stats.invoices.byStore[inv.punto_vendita].total++;
        if (inv.stato === 'consegnato') stats.invoices.byStore[inv.punto_vendita].consegnate++;
        if (inv.stato === 'in_consegna') stats.invoices.byStore[inv.punto_vendita].pending++;
      }
      if (!stats.invoices.byStatus[inv.stato]) stats.invoices.byStatus[inv.stato] = 0;
      stats.invoices.byStatus[inv.stato]++;
    });

    movimentazioni.forEach(mov => {
      if (mov.origine) {
        if (!stats.movimentazioni.byStore[mov.origine]) stats.movimentazioni.byStore[mov.origine] = 0;
        stats.movimentazioni.byStore[mov.origine]++;
      }
    });

    res.json({ success: true, stats });
  } catch (error) {
    console.error('❌ Errore dashboard admin:', error);
    res.status(500).json({ error: 'Impossibile caricare la dashboard admin' });
  }
});

app.get('/api/admin/invoices', authenticateToken, requireAdmin, async (req, res) => {
  console.log('🔄 GET /api/admin/invoices ricevuta');
  try {
    let data = await loadAllSheetData();
    const { store, status, dateFrom, dateTo } = req.query;

    if (store && store !== 'ALL') data = data.filter(inv => inv.punto_vendita === store);
    if (status && status !== 'ALL') data = data.filter(inv => inv.stato === status);
    if (dateFrom) data = data.filter(inv => inv.data_emissione >= dateFrom);
    if (dateTo) data = data.filter(inv => inv.data_emissione <= dateTo);

    res.json({ success: true, data });
  } catch (error) {
    console.error('❌ Errore caricamento fatture admin:', error);
    res.status(500).json({ error: 'Impossibile caricare le fatture globali' });
  }
});

app.get('/api/admin/movimentazioni', authenticateToken, requireAdmin, async (req, res) => {
  console.log('🔄 GET /api/admin/movimentazioni ricevuta');
  try {
    let data = await loadAllMovimentazioniData();
    const { store, dateFrom, dateTo } = req.query;

    if (store && store !== 'ALL') data = data.filter(mov => mov.origine === store);
    if (dateFrom) data = data.filter(mov => mov.data_movimento >= dateFrom);
    if (dateTo) data = data.filter(mov => mov.data_movimento <= dateTo);

    res.json({ success: true, data });
  } catch (error) {
    console.error('❌ Errore caricamento movimentazioni admin:', error);
    res.status(500).json({ error: 'Impossibile caricare le movimentazioni globali' });
  }
});

app.get('/api/admin/stores', authenticateToken, requireAdmin, async (req, res) => {
  console.log('🔄 GET /api/admin/stores ricevuta');
  try {
    const [invoices, movimentazioni] = await Promise.all([
      loadAllSheetData(),
      loadAllMovimentazioniData()
    ]);

    const storeStats = {};

    invoices.forEach(inv => {
      if (inv.punto_vendita) {
        if (!storeStats[inv.punto_vendita]) {
          storeStats[inv.punto_vendita] = { name: inv.punto_vendita, invoices: 0, movimentazioni: 0, lastActivity: null };
        }
        storeStats[inv.punto_vendita].invoices++;
        const activityDate = new Date(inv.data_consegna || inv.data_emissione);
        if (!storeStats[inv.punto_vendita].lastActivity || activityDate > storeStats[inv.punto_vendita].lastActivity) {
          storeStats[inv.punto_vendita].lastActivity = activityDate;
        }
      }
    });

    movimentazioni.forEach(mov => {
      if (mov.origine) {
        if (!storeStats[mov.origine]) {
          storeStats[mov.origine] = { name: mov.origine, invoices: 0, movimentazioni: 0, lastActivity: null };
        }
        storeStats[mov.origine].movimentazioni++;
        const activityDate = new Date(mov.timestamp);
        if (!storeStats[mov.origine].lastActivity || activityDate > storeStats[mov.origine].lastActivity) {
          storeStats[mov.origine].lastActivity = activityDate;
        }
      }
    });

    const stores = Object.values(storeStats).sort((a, b) => a.name.localeCompare(b.name));
    res.json({ success: true, stores });
  } catch (error) {
    console.error('❌ Errore caricamento negozi admin:', error);
    res.status(500).json({ error: 'Impossibile caricare i negozi' });
  }
});

app.get('/api/admin/users', authenticateToken, requireAdmin, async (req, res) => {
  console.log('🔄 GET /api/admin/users ricevuta');
  try {
    const safeUsers = users.map(user => ({
      id: user.id,
      name: user.name,
      email: user.email,
      puntoVendita: user.puntoVendita,
      role: user.role,
      permissions: user.permissions || []
    }));
    res.json({ success: true, users: safeUsers });
  } catch (error) {
    console.error('❌ Errore caricamento utenti admin:', error);
    res.status(500).json({ error: 'Impossibile caricare gli utenti' });
  }
});

app.get('/api/admin/export', authenticateToken, requireAdmin, async (req, res) => {
  console.log('🔄 GET /api/admin/export ricevuta');
  try {
    const { type, format } = req.query; // type: invoices|movimentazioni|all, format: json|csv
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
  } catch (error) {
    console.error('❌ Errore export admin:', error);
    res.status(500).json({ error: 'Impossibile completare l\'export' });
  }
});

// ==========================================
// ROUTES - UTILITÀ E FILE TXT
// ==========================================
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
    console.error('❌ Errore caricamento lista file TXT:', error);
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
    console.error('❌ Errore download file TXT:', error);
    res.status(500).json({ error: 'Impossibile scaricare il file TXT' });
  }
});

// Leggi contenuto
app.get('/api/txt-files/:filename/content', authenticateToken, async (req, res) => {
  try {
    const { filename } = req.params;
    if (!filename.endsWith('.txt') || filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: 'Nome file non valido' });
    }
    const filePath = path.join(TXT_FILES_DIR, filename);
    await fs.access(filePath);
    const fileContent = await fs.readFile(filePath, 'utf8');

    // Controlla se il file ha errori dal nome
    const hasErrors = filename.includes('_ERRORI');
    
    // Se ha errori, cerca i dettagli dalla fattura corrispondente
    let errorDetails = null;
    if (hasErrors) {
      try {
        // Estrai info dal nome file per trovare la fattura corrispondente
        const parts = filename.replace('_ERRORI.txt', '').split('_');
        const numeroDoc = parts[0];
        
        // Carica tutte le fatture e trova quella corrispondente
        const allInvoices = await loadAllSheetData();
        const relatedInvoice = allInvoices.find(inv => 
          inv.numero === numeroDoc && inv.note && inv.note.trim() !== ''
        );
        
        if (relatedInvoice) {
          errorDetails = {
            note_errori: relatedInvoice.note,
            data_consegna: relatedInvoice.data_consegna,
            confermato_da: relatedInvoice.confermato_da,
            fornitore: relatedInvoice.fornitore,
            numero: relatedInvoice.numero
          };
        }
      } catch (searchError) {
        console.warn('Impossibile trovare dettagli errore per:', filename);
      }
    }

    res.json({ 
      success: true, 
      filename, 
      content: fileContent, 
      size: fileContent.length,
      hasErrors,
      errorDetails 
    });
  } catch (error) {
    if (error.code === 'ENOENT') return res.status(404).json({ error: 'File non trovato' });
    console.error('❌ Errore lettura contenuto file TXT:', error);
    res.status(500).json({ error: 'Impossibile leggere il contenuto del file TXT' });
  }
});

// Aggiorna contenuto
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

    // backup
    const backupPath = path.join(TXT_FILES_DIR, `${filename}.backup.${Date.now()}`);
    const originalContent = await fs.readFile(filePath, 'utf8');
    await fs.writeFile(backupPath, originalContent, 'utf8');

    await fs.writeFile(filePath, content, 'utf8');
    res.json({ success: true, message: 'File aggiornato con successo', filename, size: content.length, backup_created: true });
  } catch (error) {
    if (error.code === 'ENOENT') return res.status(404).json({ error: 'File non trovato' });
    console.error('❌ Errore aggiornamento file TXT:', error);
    res.status(500).json({ error: 'Impossibile aggiornare il file TXT' });
  }
});

// Elimina file
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
    console.error('❌ Errore eliminazione file TXT:', error);
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

    // Estrai la prima data YYYY-MM-DD dal nome file con regex (funziona per entrambi i formati)
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
      const m = filename.match(/(\d{4}-\d{2}-\d{2})/); // prende la prima data nel nome
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
