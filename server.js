// server.js - VERSIONE CON TRACKING MODIFICHE + REPORT ERRORI DDT
const express = require('express');
const archiver = require('archiver');
const fetch = require('node-fetch');
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

console.log('🔍 VERIFICA CONFIGURAZIONE STARTUP:');
console.log('📊 PORT:', PORT);
console.log('🔐 JWT_SECRET configurato:', !!process.env.JWT_SECRET);
console.log('📊 GOOGLE_SHEET_ID:', GOOGLE_SHEET_ID ? 'CONFIGURATO' : 'MANCANTE');
console.log('🤖 GOOGLE_SERVICE_ACCOUNT_EMAIL:', googleEmailSource === 'missing' ? 'MANCANTE' : `CONFIGURATO (${googleEmailSource})`);
console.log('🔑 GOOGLE_PRIVATE_KEY:', googleKeySource === 'missing' ? 'MANCANTE' : `DISPONIBILE (${googleKeySource})`);

const TXT_FILES_DIR = path.join(__dirname, 'generated_txt_files');
const ensureTxtDir = async () => {
  try { await fs.access(TXT_FILES_DIR); }
  catch { console.log('📁 Creando cartella per file TXT:', TXT_FILES_DIR); await fs.mkdir(TXT_FILES_DIR, { recursive: true }); }
};
ensureTxtDir()
  .then(() => console.log('📁 Cartella file TXT pronta:', TXT_FILES_DIR))
  .catch(error => console.error('❌ Errore creazione cartella TXT:', error));

app.use(helmet());
app.use(cors({ origin: true, credentials: true }));
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true }));

const limiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 100, message: 'Troppe richieste da questo IP' });
app.use(limiter);

const loginLimiter = rateLimit({ windowMs: 15 * 60 * 1000, max: 5, message: 'Troppi tentativi di login. Riprova tra 15 minuti.' });

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

const sanitizeText = (input) => {
  return String(input ?? '')
    .replace(/[\u0000-\u001F\u007F]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
};
const sanitizeEmailSafe = (email) => String(email ?? '').trim();
const sanitizeDateSafe = (dateString) => String(dateString ?? '').trim();
const sanitizeDDT = (ddt) => String(ddt ?? '').trim().replace(/[^\w/.\-\/]/g, '');
const sanitizeInput = sanitizeText;

const validateEmail = (email) =>
  validator.isEmail(email) &&
  (email.includes('@fradiavolopizzeria.com') || email.includes('@azienda.it'));

const validateDate = (dateString) =>
  validator.isDate(dateString) && new Date(dateString) <= new Date();

// ==========================================
// ✅ FUNZIONI: Parse storico modifiche
// ==========================================
const parseStoricoModifiche = (storicoString) => {
  if (!storicoString || storicoString.trim() === '') return [];
  
  try {
    const parsed = JSON.parse(storicoString);
    return Array.isArray(parsed) ? parsed : [];
  } catch (error) {
    console.error('❌ Errore parsing storico modifiche:', error);
    return [];
  }
};

const aggiungiModificaAlloStorico = (storicoAttuale, campoModificato, valorePrecedente, valoreNuovo, modificatoDa) => {
  const storico = parseStoricoModifiche(storicoAttuale);
  
  const nuovaModifica = {
    timestamp: new Date().toISOString(),
    campo: campoModificato,
    valore_precedente: valorePrecedente || '',
    valore_nuovo: valoreNuovo || '',
    modificato_da: modificatoDa,
    data_modifica: new Date().toLocaleDateString('it-IT')
  };
  
  storico.push(nuovaModifica);
  return JSON.stringify(storico);
};

// ==========================================
// ✅ NUOVO: Parser DDT multi-formato
// ==========================================
const parseRigaDDT = (riga) => {
  riga = riga.trim();
  if (!riga) return null;
  
  // FORMATO 1: pipe separato (CODICE | NOME | UM | QTA)
  // Es: D7264 | BIB PEPSI COLA REGULAR 33 CL X 24 VP | KAR | 3
  if (riga.includes('|')) {
    const parts = riga.split('|').map(p => p.trim());
    if (parts.length === 4) {
      return {
        codice: parts[0],
        nome: parts[1],
        um: parts[2],
        quantita: parseFloat(parts[3])
      };
    }
  }
  
  // FORMATO 2: underscore + trattino (CODICE_NOME - QTA UM)
  // Es: 19332_FETTINE EXTRA CARCIOFO OLIO"ARCO"FT.3 KG - 33 KG
  if (riga.includes('_') && riga.includes(' - ')) {
    const [codiceENome, qtaEUm] = riga.split(' - ');
    const underscoreIndex = codiceENome.indexOf('_');
    
    if (underscoreIndex > 0) {
      const codice = codiceENome.substring(0, underscoreIndex).trim();
      const nome = codiceENome.substring(underscoreIndex + 1).trim();
      
      const qtaParts = qtaEUm.trim().split(' ');
      const quantita = parseFloat(qtaParts[0]);
      const um = qtaParts[qtaParts.length - 1]; // ultima parte
      
      if (!isNaN(quantita) && um) {
        return { codice, nome, um, quantita };
      }
    }
  }
  
  console.warn('⚠️ Formato riga DDT non riconosciuto:', riga);
  return null;
};

const parseDDTCompleto = (testoDDT) => {
  if (!testoDDT || testoDDT.trim() === '') return [];
  
  const righe = testoDDT.split('\n').filter(r => r.trim() !== '');
  
  return righe.map((riga, index) => {
    const parsed = parseRigaDDT(riga);
    if (!parsed) {
      console.warn(`⚠️ Riga ${index + 1} non riconosciuta:`, riga);
      return null;
    }
    return {
      ...parsed,
      riga_originale: riga,
      riga_numero: index + 1
    };
  }).filter(Boolean); // rimuovi i null
};

// ==========================================
// GENERAZIONE FILE TXT (per fatture consegnate)
// ==========================================
const generateTxtFile = async (invoiceData, isModification = false) => {
  try {
    console.log('📄 Generando file TXT per fattura:', invoiceData.id, isModification ? '(MODIFICA)' : '(NUOVA)');

    const numeroDocumento = invoiceData.numero;
    const dataConsegna = invoiceData.data_consegna;
    const nomeFornitore = invoiceData.fornitore;
    const puntoVendita = invoiceData.punto_vendita;
    const contenutoTxt = invoiceData.txt || '';
    const noteErrori = invoiceData.note || '';
    const itemNoConv = invoiceData.item_noconv || '';
    const erroriConsegna = invoiceData.errori_consegna || '';

    const negozio = negozi.find(n => n.nome === puntoVendita);
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

    const dataFormatted = dataConsegna;
    const numeroDocPulito = cleanForFilename(numeroDocumento);
    const nomeFornitorePulito = cleanForFilename(nomeFornitore);
    const codicePVPulito = cleanForFilename(codicePV);

    const hasStructuredErrors = erroriConsegna && erroriConsegna.trim() !== '';
    const hasNoteErrors = noteErrori && noteErrori.trim() !== '';
    const hasConversionErrors = itemNoConv && itemNoConv.trim() !== '';
    const hasErrors = hasStructuredErrors || hasNoteErrors || hasConversionErrors;
    
    const errorSuffix = hasErrors ? '_ERRORI' : '';

    const fileName = `${numeroDocPulito}_${dataFormatted}_${nomeFornitorePulito}_${codicePVPulito}${errorSuffix}.txt`;
    const filePath = path.join(TXT_FILES_DIR, fileName);

    // ✅ FIX: Dichiara existingFiles all'inizio della funzione
    let existingFiles = [];

    // NUOVO: Cerca e rimuovi file esistenti per questa fattura
    try {
      const allFiles = await fs.readdir(TXT_FILES_DIR);
      existingFiles = allFiles.filter(file => {
        // Cerca file che iniziano con lo stesso numero documento
        return file.startsWith(numeroDocPulito + '_') && 
               file.endsWith('.txt') && 
               !file.includes('.backup');
      });

      if (existingFiles.length > 0) {
        console.log(`🔄 Trovati ${existingFiles.length} file esistenti per questa fattura:`);
        
        for (const oldFile of existingFiles) {
          const oldFilePath = path.join(TXT_FILES_DIR, oldFile);
          
          // Crea backup del file vecchio
          const backupPath = path.join(TXT_FILES_DIR, `REPLACED_${oldFile}.backup.${Date.now()}`);
          const oldContent = await fs.readFile(oldFilePath, 'utf8');
          await fs.writeFile(backupPath, oldContent, 'utf8');
          
          // Elimina il file vecchio
          await fs.unlink(oldFilePath);
          console.log(`   ✅ Rimosso file vecchio: ${oldFile} (backup creato)`);
        }
      }
    } catch (cleanupError) {
      console.warn('⚠️ Errore durante la pulizia dei file vecchi:', cleanupError.message);
      // Non blocchiamo la generazione del nuovo file
    }

    // Crea il nuovo file
    await fs.writeFile(filePath, contenutoTxt, 'utf8');

    if (hasErrors) {
      console.log(`⚠️ File TXT CON ERRORI generato: ${fileName}`);
      if (hasStructuredErrors) console.log(`   → Errori strutturati presenti`);
      if (hasNoteErrors) console.log(`   → Errore consegna: ${noteErrori}`);
      if (hasConversionErrors) console.log(`   → Errore conversione: ${itemNoConv}`);
    } else {
      console.log(`✅ File TXT generato con successo: ${fileName}`);
    }

    if (isModification) {
      console.log(`🔄 File TXT rigenerato per modifica: ${fileName}`);
    }

    return {
      fileName,
      filePath,
      size: contenutoTxt.length,
      hasErrors,
      noteErrori: hasErrors ? (noteErrori || itemNoConv) : null,
      isModification,
      replacedFiles: existingFiles.length  // ✅ FIX: Ora existingFiles è sempre definito
    };
  } catch (error) {
    console.error('❌ Errore generazione file TXT:', error);
    throw error;
  }
};

const users = [
  { 
    id: 1, 
    name: "FDV Office", 
    email: process.env.USER_OFFICE_EMAIL, 
    password: process.env.USER_OFFICE_PASSWORD, 
    puntoVendita: "FDV Office", 
    role: "admin" 
  },
  { 
    id: 999, 
    name: "Admin Fradiavolo", 
    email: process.env.USER_ADMIN_EMAIL, 
    password: process.env.USER_ADMIN_PASSWORD, 
    puntoVendita: "ADMIN_GLOBAL", 
    role: "admin", 
    permissions: ["view_all","edit_all","manage_users","analytics","reports","system_config"], 
    storeAccess: 'global' 
  },
  { 
    id: 101, 
    name: "FDV Genova Castello", 
    email: process.env.USER_GENOVA_CASTELLO_EMAIL, 
    password: process.env.USER_GENOVA_CASTELLO_PASSWORD, 
    puntoVendita: "FDV Genova Castello", 
    role: "operator" 
  },
  { 
    id: 128, 
    name: "FDV Genova Mare", 
    email: process.env.USER_GENOVA_MARE_EMAIL, 
    password: process.env.USER_GENOVA_MARE_PASSWORD, 
    puntoVendita: "FDV Genova Mare", 
    role: "operator" 
  },
  { 
    id: 113, 
    name: "FDV Milano Sempione", 
    email: process.env.USER_MILANO_SEMPIONE_EMAIL, 
    password: process.env.USER_MILANO_SEMPIONE_PASSWORD, 
    puntoVendita: "FDV Milano Sempione", 
    role: "operator" 
  },
  { 
    id: 120, 
    name: "FDV Milano Isola", 
    email: process.env.USER_MILANO_ISOLA_EMAIL, 
    password: process.env.USER_MILANO_ISOLA_PASSWORD, 
    puntoVendita: "FDV Milano Isola", 
    role: "operator" 
  },
  { 
    id: 121, 
    name: "FDV Milano Citylife", 
    email: process.env.USER_MILANO_CITYLIFE_EMAIL, 
    password: process.env.USER_MILANO_CITYLIFE_PASSWORD, 
    puntoVendita: "FDV Milano Citylife", 
    role: "operator" 
  },
  { 
    id: 125, 
    name: "FDV Milano Bicocca", 
    email: process.env.USER_MILANO_BICOCCA_EMAIL, 
    password: process.env.USER_MILANO_BICOCCA_PASSWORD, 
    puntoVendita: "FDV Milano Bicocca", 
    role: "operator" 
  },
  { 
    id: 127, 
    name: "FDV Milano Premuda", 
    email: process.env.USER_MILANO_PREMUDA_EMAIL, 
    password: process.env.USER_MILANO_PREMUDA_PASSWORD, 
    puntoVendita: "FDV Milano Premuda", 
    role: "operator" 
  },
  { 
    id: 131, 
    name: "FDV Milano Porta Venezia", 
    email: process.env.USER_MILANO_PORTAVENEZIA_EMAIL, 
    password: process.env.USER_MILANO_PORTAVENEZIA_PASSWORD, 
    puntoVendita: "FDV Milano Porta Venezia", 
    role: "operator" 
  },
  { 
    id: 114, 
    name: "FDV Torino Carlina", 
    email: process.env.USER_TORINO_CARLINA_EMAIL, 
    password: process.env.USER_TORINO_CARLINA_PASSWORD, 
    puntoVendita: "FDV Torino Carlina", 
    role: "operator" 
  },
  { 
    id: 117, 
    name: "FDV Torino GM", 
    email: process.env.USER_TORINO_GM_EMAIL, 
    password: process.env.USER_TORINO_GM_PASSWORD, 
    puntoVendita: "FDV Torino GM", 
    role: "operator" 
  },
  { 
    id: 123, 
    name: "FDV Torino IV Marzo", 
    email: process.env.USER_TORINO_IVMARZO_EMAIL, 
    password: process.env.USER_TORINO_IVMARZO_PASSWORD, 
    puntoVendita: "FDV Torino IV Marzo", 
    role: "operator" 
  },
  { 
    id: 130, 
    name: "FDV Torino Vanchiglia", 
    email: process.env.USER_TORINO_VANCHIGLIA_EMAIL, 
    password: process.env.USER_TORINO_VANCHIGLIA_PASSWORD, 
    puntoVendita: "FDV Torino Vanchiglia", 
    role: "operator" 
  },
  { 
    id: 136, 
    name: "FDV Torino San Salvario", 
    email: process.env.USER_TORINO_SANSALVARIO_EMAIL, 
    password: process.env.USER_TORINO_SANSALVARIO_PASSWORD, 
    puntoVendita: "FDV Torino San Salvario", 
    role: "operator" 
  },
  { 
    id: 107, 
    name: "FDV Roma Parioli", 
    email: process.env.USER_ROMA_PARIOLI_EMAIL, 
    password: process.env.USER_ROMA_PARIOLI_PASSWORD, 
    puntoVendita: "FDV Roma Parioli", 
    role: "operator" 
  },
  { 
    id: 133, 
    name: "FDV Roma Ostiense", 
    email: process.env.USER_ROMA_OSTIENSE_EMAIL, 
    password: process.env.USER_ROMA_OSTIENSE_PASSWORD, 
    puntoVendita: "FDV Roma Ostiense", 
    role: "operator" 
  },
  { 
    id: 138, 
    name: "FDV Roma Trastevere", 
    email: process.env.USER_ROMA_TRASTEVERE_EMAIL, 
    password: process.env.USER_ROMA_TRASTEVERE_PASSWORD, 
    puntoVendita: "FDV Roma Trastevere", 
    role: "operator" 
  },
  { 
    id: 106, 
    name: "FDV Bologna S.Stefano", 
    email: process.env.USER_BOLOGNA_STEFANO_EMAIL, 
    password: process.env.USER_BOLOGNA_STEFANO_PASSWORD, 
    puntoVendita: "FDV Bologna S.Stefano", 
    role: "operator" 
  },
  { 
    id: 124, 
    name: "FDV Parma", 
    email: process.env.USER_PARMA_EMAIL, 
    password: process.env.USER_PARMA_PASSWORD, 
    puntoVendita: "FDV Parma", 
    role: "operator" 
  },
  { 
    id: 132, 
    name: "FDV Modena", 
    email: process.env.USER_MODENA_EMAIL, 
    password: process.env.USER_MODENA_PASSWORD, 
    puntoVendita: "FDV Modena", 
    role: "operator" 
  },
  { 
    id: 137, 
    name: "FDV Rimini", 
    email: process.env.USER_RIMINI_EMAIL, 
    password: process.env.USER_RIMINI_PASSWORD, 
    puntoVendita: "FDV Rimini", 
    role: "operator" 
  },
  { 
    id: 122, 
    name: "FDV Arese", 
    email: process.env.USER_ARESE_EMAIL, 
    password: process.env.USER_ARESE_PASSWORD, 
    puntoVendita: "FDV Arese", 
    role: "operator" 
  },
  { 
    id: 126, 
    name: "FDV Monza", 
    email: process.env.USER_MONZA_EMAIL, 
    password: process.env.USER_MONZA_PASSWORD, 
    puntoVendita: "FDV Monza", 
    role: "operator" 
  },
  { 
    id: 135, 
    name: "FDV Brescia Centro", 
    email: process.env.USER_BRESCIA_EMAIL, 
    password: process.env.USER_BRESCIA_PASSWORD, 
    puntoVendita: "FDV Brescia Centro", 
    role: "operator" 
  },
  { 
    id: 112, 
    name: "FDV Novara", 
    email: process.env.USER_NOVARA_EMAIL, 
    password: process.env.USER_NOVARA_PASSWORD, 
    puntoVendita: "FDV Novara", 
    role: "operator" 
  },
  { 
    id: 129, 
    name: "FDV Alessandria", 
    email: process.env.USER_ALESSANDRIA_EMAIL, 
    password: process.env.USER_ALESSANDRIA_PASSWORD, 
    puntoVendita: "FDV Alessandria", 
    role: "operator" 
  },
  { 
    id: 134, 
    name: "FDV Asti", 
    email: process.env.USER_ASTI_EMAIL, 
    password: process.env.USER_ASTI_PASSWORD, 
    puntoVendita: "FDV Asti", 
    role: "operator" 
  },
  { 
    id: 119, 
    name: "FDV Varese", 
    email: process.env.USER_VARESE_EMAIL, 
    password: process.env.USER_VARESE_PASSWORD, 
    puntoVendita: "FDV Varese", 
    role: "operator" 
  }
];

console.log('👥 Utenti disponibili:', users.length);
console.log('🏢 Punti vendita configurati:', [...new Set(users.map(u => u.puntoVendita))].length);

const authenticateToken = (req, res, next) => {
  const token = (req.headers['authorization'] || '').split(' ')[1];
  if (!token) return res.status(401).json({ error: 'Token di accesso richiesto' });

  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) return res.status(403).json({ error: 'Token non valido' });
    req.user = user;
    next();
  });
};
const requireAdmin = (req, res, next) => {
  console.log('🔒 Verifica permessi admin per:', req.user.email, 'Role:', req.user.role);
  if (req.user.role !== 'admin') return res.status(403).json({ error: 'Accesso riservato agli amministratori' });
  next();
};

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
      testo_ddt: row.get('testo_ddt') || '',
      item_noconv: row.get('item_noconv') || '',
      storico_modifiche: row.get('storico_modifiche') || '',
      errori_consegna: row.get('errori_consegna') || ''
    }));

    const uniqueData = data.filter((invoice, index, self) =>
      index === self.findIndex(i => i.id === invoice.id)
    );

    return uniqueData;
  } catch (error) {
    console.error('❌ Admin: Errore caricamento dati globali:', error);
    throw error;
  }
};

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
      testo_ddt: row.get('testo_ddt') || '',
      item_noconv: row.get('item_noconv') || '',
      storico_modifiche: row.get('storico_modifiche') || '',
      errori_consegna: row.get('errori_consegna') || ''
    }));

    if (puntoVendita) data = data.filter(r => r.punto_vendita === puntoVendita);
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
      creato_da: row.get('creato_da') || '',
      ddt_number: row.get('ddt_number') || ''
    }));

    data.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    return data;
  } catch (error) {
    console.error('❌ Admin: Errore caricamento movimentazioni globali:', error);
    throw error;
  }
};

const loadMovimentazioniFromSheet = async (puntoVendita) => {
  try {
    console.log('📦 Caricamento movimentazioni per:', puntoVendita);

    let sheet;
    try {
      sheet = await getGoogleSheet('Movimentazioni');
    } catch (sheetError) {
      console.error('❌ Errore accesso foglio Movimentazioni:', sheetError.message);
      const doc = await getGoogleDoc();
      sheet = await doc.addSheet({
        title: 'Movimentazioni',
        headerValues: [
          'id', 'data_movimento', 'timestamp', 'origine', 'codice_origine',
          'prodotto', 'quantita', 'unita_misura', 'destinazione', 'codice_destinazione',
          'stato', 'txt_content', 'txt_filename', 'creato_da', 'ddt_number'
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
      creato_da: row.get('creato_da') || '',
      ddt_number: row.get('ddt_number') || ''
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

const loadProdottiFromSheet = async () => {
  try {
    console.log('📦 Caricando prodotti da database Google Sheets...');
    const PRODOTTI_SHEET_ID = '1CJhd14F8qV8nS0-SK2ENSNSkWaE21KotK2ArBjJETfk';

    const prodottiDoc = new GoogleSpreadsheet(PRODOTTI_SHEET_ID, serviceAccountAuth);
    await prodottiDoc.loadInfo();

    const sheet = prodottiDoc.sheetsByTitle?.['PRODOTTI'] || prodottiDoc.sheetsByIndex[0];
    if (!sheet) throw new Error('Tab prodotti non trovato');

    const rows = await sheet.getRows();
    console.log('📊 Righe prodotti caricate:', rows.length);

    const norm = (v) => (v ?? '').toString().trim();
    const prodotti = rows.map(r => {
      const nome   = norm(r.get('Nome') || r.get('DESCRIZIONE') || r.get('Descrizione') || r.get('nome'));
      const codice = norm(r.get('Cod.mago') || r.get('CODICE') || r.get('Codice') || r.get('SKU') || r.get('codice'));
      const uom    = norm(r.get('UMB') || r.get('UM') || r.get('UOM') || r.get('unita_misura'));
      const onoff  = norm(r.get('On.Off') ?? r.get('On') ?? r.get('Attivo') ?? r.get('active'));

      const brand      = norm(r.get('Marca') || r.get('Brand') || r.get('brand'));
      const pack       = norm(r.get('Confezione') || r.get('Pack') || r.get('Formato'));
      const materiale  = norm(r.get('Materiale') || r.get('Imballo') || r.get('Package'));

      return { nome, codice, unitaMisura: uom, onOff: onoff, brand, pack, materiale };
    });

    return prodotti.filter(p => p.nome);
  } catch (error) {
    console.error('❌ Errore caricamento prodotti da Google Sheets:', error);
    try {
      const prodottiData = require('../frontend/src/data/prodotti.json');
      return Array.isArray(prodottiData) ? prodottiData : [];
    } catch {
      return [];
    }
  }
};

// ==========================================
// ✅ UPDATE RIGA FATTURA CON TRACKING MODIFICHE
// ==========================================
const updateSheetRow = async (id, updates, modificatoDa = 'system') => {
  try {
    console.log('🔄 updateSheetRow chiamata con:', { id, updates, modificatoDa });

    const sheet = await getGoogleSheet();
    const rows = await sheet.getRows();

    const row = rows.find(r => r.get('id') === id.toString());
    if (!row) {
      console.error('❌ Riga non trovata per ID:', id);
      throw new Error('Fattura non trovata');
    }

    const valoriPrecedenti = {
      data_consegna: row.get('data_consegna') || '',
      confermato_da: row.get('confermato_da') || '',
      note: row.get('note') || '',
      stato: row.get('stato') || '',
      errori_consegna: row.get('errori_consegna') || ''  // ✅ AGGIUNTO
    };

    const storicoAttuale = row.get('storico_modifiche') || '';
    let nuovoStorico = storicoAttuale;
    let isModification = false;

    const statoCorrente = row.get('stato');
    if (statoCorrente === 'consegnato') {
      Object.keys(updates).forEach(campo => {
        const valorePrecedente = valoriPrecedenti[campo];
        const valoreNuovo = updates[campo];
        
        if (valorePrecedente !== valoreNuovo) {
          console.log(`🔄 MODIFICA RILEVATA su campo "${campo}": "${valorePrecedente}" → "${valoreNuovo}"`);
          nuovoStorico = aggiungiModificaAlloStorico(
            nuovoStorico,
            campo,
            valorePrecedente,
            valoreNuovo,
            modificatoDa
          );
          isModification = true;
        }
      });
    }

    // ✅ FIX: Includi errori_consegna nei dati per generateTxtFile
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
      note: (updates.note ?? row.get('note') ?? ''),
      item_noconv: row.get('item_noconv') || '',
      errori_consegna: updates.errori_consegna || row.get('errori_consegna') || ''  // ✅ AGGIUNTO
    };

    Object.keys(updates).forEach(key => row.set(key, updates[key]));
    
    if (isModification) {
      row.set('storico_modifiche', nuovoStorico);
      console.log('💾 Storico modifiche aggiornato in Google Sheets');
    }
    
    await row.save();

    if (updates.stato === 'consegnato' || (statoCorrente === 'consegnato' && isModification)) {
      try {
        const txtResult = await generateTxtFile(invoiceDataForTxt, isModification);
        if (txtResult) {
          if (txtResult.replacedFiles > 0) {
            console.log(`🔄 File TXT sostituito (${txtResult.replacedFiles} file vecchi rimossi):`, txtResult.fileName);
          } else if (isModification) {
            console.log('🔄 File TXT rigenerato per modifica:', txtResult.fileName);
          } else {
            console.log('✅ File TXT generato:', txtResult.fileName);
          }
        } else {
          console.log('ℹ️ File TXT non generato (contenuto vuoto)');
        }
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

const generateInvoiceFromMovimentazione = async (ddtData) => {
  try {
    console.log('📄 Generando fattura automatica da DDT:', ddtData.ddt_number);

    const sheet = await getGoogleSheet();
    const rows = await sheet.getRows();

    const existingInvoice = rows.find(row => row.get('numero') === ddtData.ddt_number);
    if (existingInvoice) {
      console.log('ℹ️ Fattura già esistente per DDT:', ddtData.ddt_number);
      return { success: true, numeroFattura: ddtData.ddt_number, fatturaId: existingInvoice.get('id'), alreadyExists: true };
    }

    const timestamp = Date.now();
    const uniqueId = `ddt_${timestamp}_${ddtData.ddt_number.replace(/\//g, '_')}`;

    const txtCombinato = ddtData.prodotti
      .map(p => p.txt_content)
      .filter(Boolean)
      .join('\n');

    const elencoProdotti = ddtData.prodotti.map(p => {
      const nome = sanitizeText(p.prodotto || '');
      const qta  = (p.quantita ?? '') !== '' ? String(p.quantita) : '';
      const um   = sanitizeText(p.unita_misura || '');
      return [nome, qta ? ` - ${qta}` : '', um ? ` ${um}` : ''].join('');
    }).join('\n');

    const fatturaData = {
      id: uniqueId,
      numero: ddtData.ddt_number,
      fornitore: sanitizeText(ddtData.origine),
      data_emissione: sanitizeDateSafe(ddtData.data_movimento),
      data_consegna: '',
      stato: 'pending',
      punto_vendita: sanitizeText(ddtData.destinazione),
      confermato_da: '',
      pdf_link: '#',
      importo_totale: '0.00',
      txt: txtCombinato,
      codice_fornitore: ddtData.codice_origine || 'TRANSFER',
      item_noconv: '',
      testo_ddt: elencoProdotti,
      storico_modifiche: '',
      errori_consegna: ''
    };

    await sheet.addRow(fatturaData);
    console.log('✅ Fattura automatica creata:', ddtData.ddt_number);

    return { success: true, numeroFattura: ddtData.ddt_number, fatturaId: uniqueId };
  } catch (error) {
    console.error('❌ Errore generazione fattura da movimentazione:', error);
    return { success: false, error: error.message };
  }
};

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

// ==========================================
// ✅ NUOVO: Endpoint per parsing DDT
// ==========================================
app.get('/api/invoices/:id/parse-ddt', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    
    console.log('🔄 GET /api/invoices/:id/parse-ddt ricevuta per ID:', id);
    
    const sheet = await getGoogleSheet();
    const rows = await sheet.getRows();
    const row = rows.find(r => r.get('id') === id.toString());
    
    if (!row) {
      return res.status(404).json({ error: 'Fattura non trovata' });
    }
    
    const testoDDT = row.get('testo_ddt') || '';
    const prodottiParsed = parseDDTCompleto(testoDDT);
    
    console.log(`✅ DDT parsato: ${prodottiParsed.length} prodotti trovati`);
    
    res.json({
      success: true,
      fattura: {
        id: row.get('id'),
        numero: row.get('numero'),
        fornitore: row.get('fornitore'),
        data_emissione: row.get('data_emissione'),
        punto_vendita: row.get('punto_vendita'),
        codice_fornitore: row.get('codice_fornitore') || ''
      },
      prodotti: prodottiParsed,
      testo_originale: testoDDT
    });
  } catch (error) {
    console.error('❌ Errore parsing DDT:', error);
    res.status(500).json({ error: 'Impossibile parsare il DDT: ' + error.message });
  }
});

// ==========================================
// ✅ NUOVO: Endpoint per report errori
// ==========================================
app.post('/api/invoices/:id/report-error', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { data_consegna, modifiche_righe, note_testuali } = req.body;
    
    console.log('🔄 POST /api/invoices/:id/report-error ricevuta per ID:', id);
    
    // Validazione
    if (!data_consegna || !validateDate(data_consegna)) {
      return res.status(400).json({ error: 'Data di consegna non valida' });
    }
    
    // Almeno una modifica O una nota
    const hasModifiche = modifiche_righe?.some(m => m.modificato);
    const hasNote = note_testuali && note_testuali.trim() !== '';
    
    if (!hasModifiche && !hasNote) {
      return res.status(400).json({ 
        error: 'Inserisci almeno una modifica o una nota testuale' 
      });
    }
    
    // Recupera dati fattura
    const sheet = await getGoogleSheet();
    const rows = await sheet.getRows();
    const row = rows.find(r => r.get('id') === id.toString());
    
    if (!row) {
      return res.status(404).json({ error: 'Fattura non trovata' });
    }
    
    // Prepara oggetto errori
    const erroriData = {
      timestamp: new Date().toISOString(),
      data_consegna: sanitizeDateSafe(data_consegna),
      utente: req.user.email,
      modifiche: modifiche_righe?.filter(m => m.modificato) || [],
      note_testuali: sanitizeText(note_testuali || ''),
      righe_modificate: modifiche_righe?.filter(m => m.modificato).length || 0,
      totale_righe: modifiche_righe?.length || 0
    };
    
    console.log(`⚠️ Registrando errori: ${erroriData.righe_modificate} righe modificate`);
    
    // ✅ Converti erroriData in JSON string per salvarlo
    const erroriConsegnaJson = JSON.stringify(erroriData);
    
    // Salva in Google Sheet
    row.set('errori_consegna', erroriConsegnaJson);
    row.set('stato', 'consegnato');
    row.set('data_consegna', sanitizeDateSafe(data_consegna));
    row.set('confermato_da', req.user.email);
    await row.save();
    
    console.log('💾 Errori salvati in Google Sheet');
    
    // Prepara payload per endpoint esterno
    const externalPayload = {
      numero_documento: row.get('numero'),
      data_emissione: row.get('data_emissione'),
      data_consegna: sanitizeDateSafe(data_consegna),
      fornitore: row.get('fornitore'),
      punto_vendita: row.get('punto_vendita'),
      codice_fornitore: row.get('codice_fornitore') || '',
      testo_ddt_originale: row.get('testo_ddt') || '',
      errori: erroriData
    };

    // 📧 Crea contenuto email per fornitore
    const emailFornitore = {
      subject: `Fradiavolo - Segnalazione errore di consegna - DDT ${row.get('numero')}`,
      
      html: `
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
          <h2 style="color: #dc3545; border-bottom: 3px solid #dc3545; padding-bottom: 10px;">
            🍕 Fradiavolo - Segnalazione errore di consegna
          </h2>
          
          <div style="background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0;">
            <p style="margin: 8px 0;"><strong>Punto vendita:</strong> ${row.get('punto_vendita')}</p>
            <p style="margin: 8px 0;"><strong>Numero documento:</strong> ${row.get('numero')}</p>
            <p style="margin: 8px 0;"><strong>Data di emissione:</strong> ${new Date(row.get('data_emissione')).toLocaleDateString('it-IT')}</p>
            <p style="margin: 8px 0;"><strong>Data di consegna:</strong> ${new Date(data_consegna).toLocaleDateString('it-IT')}</p>
          </div>
          
          <h3 style="color: #495057; border-bottom: 2px solid #ffc107; padding-bottom: 8px;">
            📄 DDT emesso:
          </h3>
          <pre style="background: #e9ecef; padding: 15px; border-radius: 4px; font-size: 12px; white-space: pre-wrap; overflow-x: auto;">${row.get('testo_ddt') || 'Non disponibile'}</pre>
          
          <h3 style="color: #495057; border-bottom: 2px solid #dc3545; padding-bottom: 8px; margin-top: 30px;">
            ⚠️ Errori segnalati:
          </h3>
          ${erroriData.modifiche && erroriData.modifiche.length > 0 ? `
            <div style="margin: 15px 0;">
              ${erroriData.modifiche.map(modifica => `
                <div style="background: #fff3cd; border-left: 4px solid #ffc107; padding: 12px; margin: 10px 0; border-radius: 4px;">
                  <p style="margin: 5px 0; font-weight: bold;">Riga ${modifica.riga_numero}: ${modifica.nome || modifica.prodotto_originale || 'Prodotto'}</p>
                  <p style="margin: 5px 0; color: #856404;">
                    Codice: ${modifica.codice || 'N/A'} | 
                    Ordinato: ${modifica.quantita_originale} ${modifica.unita_misura || ''} → 
                    Ricevuto: ${modifica.quantita_ricevuta} ${modifica.unita_misura || ''}
                  </p>
                  ${modifica.motivo ? `<p style="margin: 5px 0; font-style: italic; color: #666;">Motivo: ${modifica.motivo}</p>` : ''}
                </div>
              `).join('')}
            </div>
          ` : '<p style="color: #6c757d;">Nessuna modifica alle quantità</p>'}
          
          <h3 style="color: #495057; border-bottom: 2px solid #28a745; padding-bottom: 8px; margin-top: 30px;">
            📝 Note aggiuntive:
          </h3>
          <div style="background: white; padding: 15px; border: 1px solid #dee2e6; border-radius: 4px;">
            ${erroriData.note_testuali || '<em style="color: #6c757d;">Nessuna nota aggiuntiva</em>'}
          </div>
          
          <div style="margin-top: 30px; padding: 15px; background: #e9ecef; border-radius: 4px; text-align: center;">
            <p style="margin: 0; color: #6c757d; font-size: 12px;">
              Segnalato da: ${erroriData.utente}<br>
              Data segnalazione: ${new Date(erroriData.timestamp).toLocaleString('it-IT')}
            </p>
          </div>
        </div>
      `,
      
      text: `
FRADIAVOLO - SEGNALAZIONE ERRORE DI CONSEGNA
=============================================

Punto vendita: ${row.get('punto_vendita')}
Numero documento: ${row.get('numero')}
Data di emissione: ${new Date(row.get('data_emissione')).toLocaleDateString('it-IT')}
Data di consegna: ${new Date(data_consegna).toLocaleDateString('it-IT')}

DDT EMESSO:
-----------
${row.get('testo_ddt') || 'Non disponibile'}

ERRORI SEGNALATI:
-----------------
${erroriData.modifiche && erroriData.modifiche.length > 0 
  ? erroriData.modifiche.map(m => 
      `• Riga ${m.riga_numero}: ${m.nome || m.prodotto_originale || 'Prodotto'}
   Codice: ${m.codice || 'N/A'}
   Ordinato: ${m.quantita_originale} ${m.unita_misura || ''} → Ricevuto: ${m.quantita_ricevuta} ${m.unita_misura || ''}
   ${m.motivo ? 'Motivo: ' + m.motivo : ''}`
    ).join('\n\n')
  : 'Nessuna modifica alle quantità'}

NOTE AGGIUNTIVE:
----------------
${erroriData.note_testuali || 'Nessuna nota aggiuntiva'}

---
Segnalato da: ${erroriData.utente}
Data: ${new Date(erroriData.timestamp).toLocaleString('it-IT')}
      `.trim()
    };

    externalPayload.email_fornitore = emailFornitore;
    
    // Chiamata endpoint esterno n8n
    try {
      console.log('📡 Chiamando webhook n8n...');
      const webhookResponse = await fetch(
        'https://andreafd.app.n8n.cloud/webhook-test/198ebdde-faee-4d0f-93aa-9c8dc138bbee',
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(externalPayload)
        }
      );
      
      if (webhookResponse.ok) {
        console.log('✅ Errori inviati a webhook n8n con successo');
      } else {
        console.warn('⚠️ Webhook n8n ha risposto con errore:', webhookResponse.status);
      }
    } catch (webhookError) {
      console.error('❌ Errore chiamata webhook n8n:', webhookError.message);
    }
    
    // ✅ FIX: Genera file TXT con errori_consegna incluso
    const invoiceDataForTxt = {
      id: row.get('id'),
      numero: row.get('numero'),
      fornitore: row.get('fornitore'),
      data_emissione: row.get('data_emissione'),
      data_consegna: sanitizeDateSafe(data_consegna),
      punto_vendita: row.get('punto_vendita'),
      confermato_da: req.user.email,
      txt: row.get('txt') || '',
      codice_fornitore: row.get('codice_fornitore') || '',
      note: sanitizeText(note_testuali || ''),
      item_noconv: row.get('item_noconv') || '',
      errori_consegna: erroriConsegnaJson  // ✅ AGGIUNTO: Passa il JSON degli errori
    };
    
    try {
      const txtResult = await generateTxtFile(invoiceDataForTxt);
      if (txtResult) {
        console.log('📄 File TXT generato con errori segnalati:', txtResult.fileName);
        if (txtResult.replacedFiles > 0) {
          console.log(`   🔄 ${txtResult.replacedFiles} file vecchi sostituiti`);
        }
      } else {
        console.log('⚠️ File TXT non generato (contenuto vuoto)');
      }
    } catch (txtError) {
      console.error('❌ Errore generazione file TXT:', txtError);
    }
    
    res.json({ 
      success: true, 
      message: '⚠️ Errori registrati e comunicati con successo',
      webhook_chiamato: true
    });
    
  } catch (error) {
    console.error('❌ Errore report errori:', error);
    res.status(500).json({ error: 'Impossibile registrare gli errori: ' + error.message });
  }
});

app.post('/api/invoices/:id/confirm', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const body = req.body ?? {};

    const data_consegna_raw = body.data_consegna;
    const confermato_da_email_raw = body.confermato_da_email;

    if (!id || !data_consegna_raw) {
      return res.status(400).json({ error: 'ID fattura e data consegna richiesti' });
    }
    if (!validateDate(data_consegna_raw)) {
      return res.status(400).json({ error: 'Data non valida' });
    }

    let confermatoDa = req.user.email;
    if (req.user.role === 'admin' && typeof confermato_da_email_raw === 'string' && validateEmail(confermato_da_email_raw)) {
      confermatoDa = sanitizeEmailSafe(confermato_da_email_raw);
    }

    const noteFromBody =
      (typeof body.note_errori === 'string' && body.note_errori.trim() !== '' ? body.note_errori :
      (typeof body.note === 'string' && body.note.trim() !== '' ? body.note : ''));

    const updates = {
      stato: 'consegnato',
      data_consegna: sanitizeDateSafe(data_consegna_raw),
      confermato_da: confermatoDa,
      ...(noteFromBody ? { note: sanitizeText(noteFromBody) } : {})
    };

    await updateSheetRow(String(id), updates, confermatoDa);
    res.json({ success: true, message: 'Consegna confermata' });
  } catch (error) {
    console.error('Errore conferma:', error);
    res.status(500).json({ error: 'Impossibile confermare la consegna' });
  }
});

app.put('/api/invoices/:id', authenticateToken, async (req, res) => {
  try {
    const { id } = req.params;
    const { data_consegna, confermato_da, note, errori_consegna } = req.body;
    
    console.log('🔄 PUT /api/invoices/:id ricevuta per ID:', id);
    console.log('📦 Body:', { data_consegna, confermato_da, note: note ? 'presente' : 'assente', errori_consegna: errori_consegna ? 'presente' : 'assente' });
    
    if (!id) return res.status(400).json({ error: 'ID fattura richiesto' });

    const updates = {};
    
    if (data_consegna) {
      if (!validateDate(data_consegna)) return res.status(400).json({ error: 'Data non valida' });
      updates.data_consegna = sanitizeDateSafe(data_consegna);
    }
    
    if (confermato_da) {
      if (!validateEmail(confermato_da)) return res.status(400).json({ error: 'Email non valida' });
      updates.confermato_da = sanitizeEmailSafe(confermato_da);
    }
    
    if (typeof note === 'string') {
      updates.note = sanitizeText(note);
    }
    
    // ✅ NUOVO: Gestisci aggiornamento errori_consegna
    if (typeof errori_consegna === 'string' && errori_consegna.trim() !== '') {
      updates.errori_consegna = errori_consegna;
      console.log('✅ Aggiornamento errori_consegna ricevuto');
    }

    if (Object.keys(updates).length === 0) {
      return res.status(400).json({ error: 'Nessun campo da aggiornare' });
    }

    await updateSheetRow(id, updates, req.user.email);
    
    console.log('✅ Fattura aggiornata con successo');
    
    res.json({ 
      success: true, 
      message: 'Fattura aggiornata con successo', 
      updated_fields: Object.keys(updates) 
    });
  } catch (error) {
    console.error('❌ Errore aggiornamento fattura:', error);
    res.status(500).json({ error: 'Impossibile aggiornare la fattura: ' + error.message });
  }
});

const saveMovimentazioniToSheet = async (movimenti, origine, ddtNumber) => {
  try {
    console.log('📦 Salvando movimentazioni su Google Sheets...');
    console.log('📄 DDT Number:', ddtNumber);
    console.log('📦 Numero prodotti:', movimenti.length);

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
          'stato', 'txt_content', 'txt_filename', 'creato_da', 'ddt_number'
        ]
      });
    }

    const timestamp = new Date().toISOString();
    const dataOggi = new Date().toLocaleDateString('it-IT');

    const righe = movimenti.map((movimento, index) => ({
      id: `${ddtNumber.replace(/\//g, '_')}_${index}`,
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
      creato_da: movimento.creato_da || '',
      ddt_number: ddtNumber
    }));

    await sheet.addRows(righe);
    console.log(`✅ ${righe.length} righe salvate nel foglio Movimentazioni`);

    const ddtData = {
      ddt_number: ddtNumber,
      origine: movimenti[0].origine || origine,
      codice_origine: movimenti[0].codice_origine || '',
      destinazione: movimenti[0].destinazione,
      codice_destinazione: movimenti[0].codice_destinazione || '',
      data_movimento: dataOggi,
      prodotti: righe
    };

    const fatturaResult = await generateInvoiceFromMovimentazione(ddtData);

    return {
      success: true,
      righe_inserite: righe.length,
      fattura_generata: fatturaResult.success ? 1 : 0,
      fattura_gia_esistente: fatturaResult.alreadyExists || false,
      dettagli_fattura: fatturaResult
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
    const { movimenti, origine, creato_da_email, ddt_number } = req.body;

    if (!movimenti || !Array.isArray(movimenti) || movimenti.length === 0) {
      return res.status(400).json({ error: 'Lista movimenti richiesta' });
    }
    if (!origine || origine.trim() === '') {
      return res.status(400).json({ error: 'Punto vendita di origine richiesto' });
    }
    if (!ddt_number || ddt_number.trim() === '') {
      return res.status(400).json({ error: 'Numero DDT richiesto' });
    }
    if (req.user.puntoVendita !== origine && req.user.role !== 'admin') {
      return res.status(403).json({ error: 'Non autorizzato per questo punto vendita' });
    }

    let customOrigin = origine;
    let customCreatoDa = req.user.email;
    if (req.user.role === 'admin' && creato_da_email && validateEmail(creato_da_email)) {
      const userObj = users.find(u => u.email === creato_da_email);
      if (userObj) {
        customOrigin = userObj.puntoVendita;
        customCreatoDa = userObj.email;
      }
    }

    const ddtNumberClean = sanitizeDDT(ddt_number);

    for (let i = 0; i < movimenti.length; i++) {
      const m = movimenti[i];

      if (!m.prodotto || m.prodotto.trim() === '') {
        return res.status(400).json({ error: `Prodotto richiesto per movimento ${i + 1}` });
      }
      if (!m.quantita || isNaN(m.quantita) || m.quantita <= 0) {
        return res.status(400).json({ error: `Quantità valida richiesta per movimento ${i + 1}` });
      }
      if (!m.destinazione || m.destinazione.trim() === '') {
        return res.status(400).json({ error: `Destinazione richiesta per movimento ${i + 1}` });
      }

      m.prodotto = sanitizeText(m.prodotto);
      m.quantita = parseFloat(m.quantita);
      m.unita_misura = sanitizeText(m.unita_misura || '');
      m.destinazione = sanitizeText(m.destinazione);
      m.txt_content = typeof m.txt_content === 'string' ? m.txt_content : '';
      m.creato_da = customCreatoDa;
      m.origine = customOrigin;
      m.ddt_number = ddtNumberClean;
    }

    const result = await saveMovimentazioniToSheet(movimenti, sanitizeText(customOrigin), ddtNumberClean);

    let successMessage = `✅ DDT ${ddtNumberClean}: ${result.righe_inserite} prodotti registrati`;
    if (result.fattura_generata > 0) {
      if (result.fattura_gia_esistente) {
        successMessage += ` - Fattura già esistente nella sezione "Da Confermare"`;
      } else {
        successMessage += ` - Fattura automatica creata nella sezione "Da Confermare"`;
      }
    }

    res.json({ success: true, message: successMessage, data: result });
  } catch (error) {
    console.error('❌ Errore POST /api/movimentazioni:', error);
    res.status(500).json({ error: 'Impossibile salvare le movimentazioni' });
  }
});

app.get('/api/prodotti', authenticateToken, async (req, res) => {
  console.log('🔄 GET /api/prodotti ricevuta');
  console.log('👤 Richiesta da utente:', req.user.email);

  try {
    const { active, page, per_page } = req.query;

    const all = await loadProdottiFromSheet();

    const onlyActive = active === '1';
    let filtered = onlyActive
      ? all.filter(p => ['1', 1, true, 'TRUE', 'true'].includes(p.onOff))
      : all;

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
    console.error('❌ Errore caricamento prodotti:', error);
    res.status(500).json({ error: 'Impossibile caricare la lista prodotti', details: error.message });
  }
});

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
        pending: invoices.filter(inv => inv.stato === 'pending').length,
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
        if (inv.stato === 'pending') stats.invoices.byStore[inv.punto_vendita].pending++;
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
  } catch (error) {
    console.error('❌ Errore export admin:', error);
    res.status(500).json({ error: 'Impossibile completare l\'export' });
  }
});

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

app.get('/api/txt-files/:filename/content', authenticateToken, async (req, res) => {
  try {
    const { filename } = req.params;

    if (!filename.endsWith('.txt') || filename.includes('..') || filename.includes('/')) {
      return res.status(400).json({ error: 'Nome file non valido' });
    }

    const filePath = path.join(TXT_FILES_DIR, filename);
    await fs.access(filePath);
    const fileContent = await fs.readFile(filePath, 'utf8');
    const hasErrorSuffix = filename.includes('_ERRORI');

    const response = {
      success: true,
      filename,
      content: fileContent,
      size: fileContent.length,
      hasErrors: hasErrorSuffix,
      errorDetails: null,
      storicoModifiche: null,
      isModified: false
    };

    const cleanFilename = filename.replace('_ERRORI.txt', '').replace('.txt', '');
    const parts = cleanFilename.split('_');

    console.log(`\n🔍 DEBUG ESTRAZIONE NUMERO DOCUMENTO:`);
    console.log(`   Filename originale: ${filename}`);
    console.log(`   Filename pulito: ${cleanFilename}`);
    console.log(`   Parts array:`, parts);

    let numeroDocumento = parts[0];

    console.log(`   Numero documento estratto: ${numeroDocumento}`);

    if (numeroDocumento) {
      try {
        const allInvoices = await loadAllSheetData();
        
        console.log(`\n📊 RICERCA NEL DATABASE:`);
        console.log(`   Cerco fattura con numero: ${numeroDocumento}`);
        console.log(`   Totale fatture nel database: ${allInvoices.length}`);

        const relatedInvoice = allInvoices.find(inv => {
          const invNumero = String(inv.numero || '').trim();
          const searchNumero = String(numeroDocumento).trim();
          
          const match = invNumero === searchNumero;
          if (match) {
            console.log(`   ✅ MATCH TROVATO!`);
          }
          return match;
        });

        if (relatedInvoice) {
          console.log(`\n✅ FATTURA TROVATA:`);
          console.log(`   Numero: ${relatedInvoice.numero}`);
          console.log(`   Fornitore: ${relatedInvoice.fornitore}`);
          console.log(`   Punto vendita: ${relatedInvoice.punto_vendita}`);
          console.log(`   Stato: ${relatedInvoice.stato}`);
          
          const errorDetails = {};

          // ✅ NUOVO: Leggi errori_consegna strutturati
const erroriConsegnaValue = String(relatedInvoice.errori_consegna || '').trim();
console.log(`\n🔍 CONTROLLO ERRORI:`);
console.log(`   Colonna "errori_consegna": "${erroriConsegnaValue.substring(0, 100)}..."`);

if (erroriConsegnaValue !== '') {
  try {
    const erroriParsed = JSON.parse(erroriConsegnaValue);
    errorDetails.errori_consegna = erroriParsed;
    console.log(`   ✅ Errori consegna strutturati trovati!`);
    console.log(`   Righe modificate: ${erroriParsed.righe_modificate || 0}`);
    console.log(`   Note testuali: ${erroriParsed.note_testuali ? 'Sì' : 'No'}`);
  } catch (parseError) {
    console.warn(`   ⚠️ Errore parsing errori_consegna:`, parseError.message);
  }
} else {
  console.log(`   ℹ️ Nessun errore strutturato (errori_consegna vuoto)`);
}

// Mantieni backward compatibility con "note" (legacy)
const noteValue = String(relatedInvoice.note || '').trim();
console.log(`   Colonna "note" (legacy): "${noteValue}"`);

if (noteValue !== '') {
  errorDetails.note_errori = noteValue;
  console.log(`   ⚠️ Note errori legacy trovate!`);
} else {
  console.log(`   ✅ Nessuna nota legacy`);
}

// Mantieni backward compatibility con item_noconv
const itemNoConvValue = String(relatedInvoice.item_noconv || '').trim();
console.log(`   Colonna "item_noconv" (legacy): "${itemNoConvValue}"`);

if (itemNoConvValue !== '') {
  errorDetails.item_noconv = itemNoConvValue;
  console.log(`   ⚠️ Errore conversione trovato!`);
} else {
  console.log(`   ✅ Nessun errore conversione`);
}

          if (Object.keys(errorDetails).length > 0) {
            errorDetails.data_consegna = relatedInvoice.data_consegna;
            errorDetails.confermato_da = relatedInvoice.confermato_da;
            errorDetails.fornitore = relatedInvoice.fornitore;
            errorDetails.numero = relatedInvoice.numero;
            errorDetails.punto_vendita = relatedInvoice.punto_vendita;

            response.errorDetails = errorDetails;
            response.hasErrors = true;
            console.log(`\n✅ RISPOSTA FINALE: Errori presenti`);
            console.log(`   errorDetails:`, errorDetails);
          } else {
            console.log(`\n⚠️ RISPOSTA FINALE: Nessun errore trovato nel database`);
          }

          const storicoString = relatedInvoice.storico_modifiche || '';
          if (storicoString && storicoString.trim() !== '') {
            const storicoArray = parseStoricoModifiche(storicoString);
            if (storicoArray.length > 0) {
              response.storicoModifiche = storicoArray;
              response.isModified = true;
              console.log(`\n🔄 STORICO MODIFICHE PRESENTE:`);
              console.log(`   Numero modifiche: ${storicoArray.length}`);
              storicoArray.forEach((mod, idx) => {
                console.log(`   Modifica ${idx + 1}:`, {
                  campo: mod.campo,
                  da: mod.valore_precedente,
                  a: mod.valore_nuovo,
                  quando: mod.data_modifica,
                  chi: mod.modificato_da
                });
              });
            }
          }
        } else {
          console.log(`\n❌ FATTURA NON TROVATA`);
          console.log(`   Numero cercato: ${numeroDocumento}`);
          console.log(`   Prime 10 fatture nel database:`);
          allInvoices.slice(0, 10).forEach(inv => {
            console.log(`      - "${inv.numero}" (${inv.fornitore})`);
          });
        }
      } catch (searchError) {
        console.error('❌ Errore ricerca fattura nel database:', searchError);
      }
    } else {
      console.log(`⚠️ Impossibile estrarre numero documento da: ${filename}`);
    }

    res.json(response);

  } catch (error) {
    if (error.code === 'ENOENT') {
      return res.status(404).json({ error: 'File non trovato' });
    }
    console.error('❌ Errore lettura contenuto file TXT:', error);
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
    console.error('❌ Errore aggiornamento file TXT:', error);
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
    console.error('❌ Errore eliminazione file TXT:', error);
    res.status(500).json({ error: 'Impossibile eliminare il file TXT' });
  }
});

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

app.get('/api/health', (req, res) => {
  res.json({
    success: true,
    status: 'OK',
    timestamp: new Date().toISOString(),
    version: '1.0.0',
    txt_files_dir: TXT_FILES_DIR
  });
});

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

app.use((error, req, res, next) => {
  console.error('Errore non gestito:', error);
  res.status(500).json({ error: 'Errore interno del server' });
});

app.use('*', (req, res) => {
  console.log('❌ Route non trovata:', req.originalUrl);
  res.status(404).json({ error: 'Endpoint non trovato' });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server in esecuzione su 0.0.0.0:${PORT}`);
  console.log(`📱 Accesso mobile: http://192.168.60.142:${PORT}`);
  console.log(`🔐 JWT Secret configurato: ${!!process.env.JWT_SECRET}`);
  console.log(`📊 Google Sheets ID: ${GOOGLE_SHEET_ID}`);
  console.log(`🤖 Google Service Account configurato: ${!!GOOGLE_SERVICE_ACCOUNT_EMAIL}`);
  console.log(`📁 Cartella file TXT: ${TXT_FILES_DIR}`);
});

module.exports = app;
