import {once} from 'node:events';
const rows = Number(process.argv[2] || 1_000_000);
const categories = ['food','transport','utilities','health','entertainment'], sources = ['mobile','web','partner'];
if (!Number.isInteger(rows) || rows < 1) throw new Error('Informe uma quantidade inteira positiva de linhas.');
const stream = process.stdout; stream.write('occurred_at,category,amount,source\n');
for (let i=0; i<rows; i++) {
  const monthIndex = i % 24;
  const date = new Date(Date.UTC(2024 + Math.floor(monthIndex / 12), monthIndex % 12, i % 28 + 1, i % 24)).toISOString();
  if (!stream.write(`${date},${categories[i%categories.length]},${((i*17)%10000/100).toFixed(2)},${sources[i%sources.length]}\n`)) await once(stream, 'drain');
}
console.error(`${rows.toLocaleString()} linhas geradas.`);
