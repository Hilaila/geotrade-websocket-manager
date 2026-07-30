// Geo-Trade AI — Data Validation Engine™
// Garantit que seules des données fiables alimentent le Market Memory Engine™.
// Une instance distincte par (exchange, symbole) — voir gestionnaireValidateurs
// dans index.js — pour ne jamais comparer le prix d'un actif à celui d'un autre.
class DataValidationEngine {
  constructor() {
    this.lastPrice = null;
    this.lastTimestamp = null;
  }

  validate(data) {
    const errors = [];

    if (data.price === null || data.price === undefined) errors.push("Prix absent");
    if (!data.timestamp) errors.push("Horodatage absent");

    if (this.lastPrice && typeof data.price === 'number') {
      const variation = Math.abs((data.price - this.lastPrice) / this.lastPrice) * 100;
      if (variation > 20) errors.push("Variation prix anormale");
    }

    if (this.lastTimestamp && data.timestamp) {
      const delay = Date.now() - data.timestamp;
      if (delay > 10000) errors.push("Données trop anciennes");
    }

    // Mémoire mise à jour uniquement si la donnée est exploitable — une
    // valeur déjà rejetée ne doit pas devenir la nouvelle référence.
    if (errors.length === 0) {
      this.lastPrice = data.price;
      this.lastTimestamp = data.timestamp;
    }

    return { valid: errors.length === 0, errors, data };
  }
}

export default DataValidationEngine;
