

db.universo_nerd_raw.drop();
db.universo_nerd_raw.insertMany([
  ...db.base.find().toArray(),
  ...db.dados.find().toArray()
]);

db.universo_nerd_raw.find().forEach(doc => {
  const novo = {};
  novo.name = (doc.Name || doc.nome || doc.char_name || doc.charName || doc.Char_name || "").trim();
  novo.name = novo.name.split(" ").map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(" ");
  let alias = doc.alias || doc.Alias || doc.ALIAS;
  if (typeof alias === "string") alias = [alias];
  if (!Array.isArray(alias)) alias = [];
  alias = alias.filter(a => a && a !== null && a !== "").map(a => a.trim());
  novo.alias = [...new Set(alias)];
  let uni = (doc.universe || doc.Universe || "").trim().toLowerCase();
  if (/marvel/.test(uni)) uni = "Marvel";
  else if (/dc/.test(uni)) uni = "DC";
  else if (/star ?wars/.test(uni)) uni = "Star Wars";
  else if (/witcher/.test(uni)) uni = "The Witcher";
  else if (/halo/.test(uni)) uni = "Halo";
  else if (/pokemon/.test(uni)) uni = "Pokémon";
  else if (/zelda/.test(uni)) uni = "Legend of Zelda";
  else if (/matrix/.test(uni)) uni = "Matrix";
  else if (/avatar/.test(uni)) uni = "Avatar";
  else if (/nintendo/.test(uni)) uni = "Nintendo";
  else uni = uni.charAt(0).toUpperCase() + uni.slice(1);
  novo.universe = uni;
  let p = doc.powerLevel || doc.PowerLevel || doc.powerlevel || doc.Power_Level || doc.power_level;
  if (typeof p === "string") {
    const match = p.match(/\d+/);
    p = match ? parseInt(match[0]) : null;
  }
  novo.powerLevel = typeof p === "number" ? p : null;
  let eq = doc.equipment;
  if (typeof eq === "string") eq = eq.split(/,|;/);
  eq = (eq || []).map(e => e.trim().toLowerCase()).filter(e => e && e !== "n/a" && e !== "");
  novo.equipment = [...new Set(eq)];
  novo.species = (doc.species || "").trim();
  novo.species = novo.species.charAt(0).toUpperCase() + novo.species.slice(1).toLowerCase();
  let mv = doc.movies;
  if (typeof mv === "string") mv = mv.split(/,|;/);
  mv = (mv || []).map(m => m.trim()).filter(m => m && m.toLowerCase() !== "n/a");
  novo.movies = [...new Set(mv)];
  let year = doc.debut_year;
  if (typeof year === "string") {
    const match = year.match(/\d{4}/);
    year = match ? parseInt(match[0]) : null;
  }
  novo.debut_year = year || null;
  novo.first_appearance = doc.first_appearance || doc.First_Appearance || doc.firstAppearance || null;
  db.nerd_universe_clean.updateOne({ name: novo.name }, { $set: novo }, { upsert: true });
});
print("✅ Higienização concluída!");

// =========================
// ETAPA 2 – NORMALIZAÇÃO
// =========================

db.universes.drop();
db.species.drop();
db.equipment_list.drop();
db.movies.drop();
db.characters.drop();
db.nerd_universe_clean.aggregate([{ $group: { _id: "$universe" } }])
  .forEach(u => { if (u._id) db.universes.insertOne({ name: u._id }); });
db.nerd_universe_clean.aggregate([{ $group: { _id: "$species" } }])
  .forEach(s => { if (s._id) db.species.insertOne({ name: s._id }); });
db.nerd_universe_clean.find().forEach(ch => {
  (ch.equipment || []).forEach(eq => {
    db.equipment_list.updateOne({ name: eq }, { $setOnInsert: { name: eq } }, { upsert: true });
  });
});
db.nerd_universe_clean.find().forEach(ch => {
  (ch.movies || []).forEach(mv => {
    db.movies.updateOne({ name: mv }, { $setOnInsert: { name: mv } }, { upsert: true });
  });
});
db.nerd_universe_clean.find().forEach(ch => {
  const universe = db.universes.findOne({ name: ch.universe });
  const species = db.species.findOne({ name: ch.species });
  const equipIds = (ch.equipment || []).map(e => db.equipment_list.findOne({ name: e })._id);
  const movieIds = (ch.movies || []).map(m => db.movies.findOne({ name: m })._id);
  db.characters.insertOne({
    name: ch.name,
    alias: ch.alias,
    powerLevel: ch.powerLevel,
    debut_year: ch.debut_year,
    first_appearance: ch.first_appearance,
    universe_id: universe ? universe._id : null,
    species_id: species ? species._id : null,
    equipment_ids: equipIds,
    movie_ids: movieIds
  });
});
print("✅ Normalização concluída!");

