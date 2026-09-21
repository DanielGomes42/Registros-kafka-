import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";

const api = (path, options) =>
  fetch(`/api${path}`, options).then((r) =>
    r.ok ? r.json() : r.text().then((t) => Promise.reject(t)),
  );
const money = new Intl.NumberFormat("pt-BR", {
  style: "currency",
  currency: "BRL",
});

function App() {
  const [job, setJob] = useState(null),
    [rows, setRows] = useState([]),
    [cursors, setCursors] = useState([null]),
    [cursorIndex, setCursorIndex] = useState(0),
    [nextCursor, setNextCursor] = useState(""),
    [allAgg, setAllAgg] = useState([]),
    [agg, setAgg] = useState([]),
    [years, setYears] = useState([]),
    [year, setYear] = useState(""),
    [transactionYear, setTransactionYear] = useState(""),
    [transactionMonth, setTransactionMonth] = useState(""),
    [loadingAgg, setLoadingAgg] = useState(false),
    [capacity, setCapacity] = useState(null),
    [error, setError] = useState("");
  const load = () => {
    const filter = `${transactionYear ? `&year=${transactionYear}` : ""}${transactionMonth ? `&month=${transactionMonth}` : ""}`;
    api(
      `/transactions?size=30${cursors[cursorIndex] ? `&cursor=${cursors[cursorIndex]}` : ""}${filter}`,
    )
      .then((d) => {
        setRows(d.items);
        setNextCursor(d.nextCursor);
      })
      .catch(setError);
    api("/aggregates").then(setAllAgg).catch(setError);
    api("/capacity").then(setCapacity).catch(setError);
  };
  useEffect(load, [cursorIndex, transactionYear, transactionMonth]);
  useEffect(() => {
    api("/aggregation-years")
      .then((data) => {
        setYears(data);
        setYear((current) => current || String(data[0] || ""));
        setTransactionYear((current) => current || String(data[0] || ""));
      })
      .catch(setError);
  }, [job?.status]);
  useEffect(() => {
    setCursors([null]);
    setCursorIndex(0);
  }, [transactionYear, transactionMonth]);
  useEffect(() => {
    if (!year) return;
    setLoadingAgg(true);
    api(`/aggregations?year=${year}`)
      .then(setAgg)
      .catch(setError)
      .finally(() => setLoadingAgg(false));
  }, [year]);
  useEffect(() => {
    if (!job) return;
    if (job.status === "COMPLETED") {
      load();
      if (year) {
        setLoadingAgg(true);
        api(`/aggregations?year=${year}`)
          .then(setAgg)
          .catch(setError)
          .finally(() => setLoadingAgg(false));
      }
      return;
    }
    if (!["QUEUED", "PUBLISHING", "CONSUMING"].includes(job.status)) return;
    const timer = setTimeout(
      () => api(`/imports/${job.id}`).then(setJob).catch(setError),
      1000,
    );
    return () => clearTimeout(timer);
  }, [job]);
  async function upload(e) {
    const file = e.target.files?.[0];
    if (!file) return;
    setError("");
    const form = new FormData();
    form.append("file", file);
    try {
      setJob({
        ...(await api("/imports", { method: "POST", body: form })),
        status: "QUEUED",
        read: 0,
        published: 0,
        persisted: 0,
        errors: 0,
      });
    } catch (x) {
      setError(String(x));
    }
  }
  const currentTotal = allAgg.reduce((n, x) => n + Number(x.total), 0),
    total = allAgg.reduce((n, x) => n + Number(x.count), 0),
    categories = ["food", "transport", "utilities", "health", "entertainment"],
    months = Array.from({ length: 12 }, (_, i) =>
      new Date(2000, i, 1).toLocaleString("pt-BR", { month: "long" }),
    );
  return (
    <main>
      <header>
        <div>
          <p className="eyebrow">GEOSAPIENS</p>
          <h1>Central de dados</h1>
          <p className="download">
            Ainda não tem um arquivo?{" "}
            <a
              href="https://drive.google.com/drive/folders/1WDD22VEXNQZYTI0rhuBivW2ko5vhfjye?usp=drive_link"
              target="_blank"
              rel="noreferrer"
            >
              Baixe o CSV de teste no Google Drive
            </a>{" "}
            e depois importe-o aqui.
          </p>
        </div>
        <label className="upload">
          Importar CSV
          <input type="file" accept=".csv,text/csv" onChange={upload} />
        </label>
      </header>
      {error && <p className="error">{error}</p>}
      {job && (
        <section className={`job ${job.status.toLowerCase()}`}>
          <strong>{job.status}</strong> - {job.read.toLocaleString()} lidas,{" "}
          {job.published.toLocaleString()} no Kafka,{" "}
          {job.persisted.toLocaleString()} persistidas, {job.errors} inválidas{" "}
          {job.message && `(${job.message})`}
        </section>
      )}
      <section className="cards">
        <article>
          <span>Total processado</span>
          <b>{total.toLocaleString()}</b>
        </article>
        <article>
          <span>Valor agregado</span>
          <b>{money.format(currentTotal)}</b>
        </article>
        <article>
          <span>Grupos mês/categoria</span>
          <b>{agg.length}</b>
        </article>
        <article className={capacity?.state?.toLowerCase()}>
          <span>Uso estimado do banco</span>
          <b>{capacity ? `${capacity.usedPercent.toFixed(2)}%` : "..."}</b>
          <small>{capacity?.state || "Carregando"}</small>
        </article>
        <article>
          <span>Registros estimados restantes</span>
          <b>
            {capacity
              ? Number(capacity.estimatedAdditionalRecords).toLocaleString()
              : "..."}
          </b>
          <small>Limite configurado</small>
        </article>
      </section>
      <section className="panel">
        <div className="aggregation-head">
          <h2>Agregação por mês e categoria</h2>
          <label>
            Ano:{" "}
            <select
              value={year}
              onChange={(e) => setYear(e.target.value)}
              disabled={!years.length}
            >
              {years.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
        </div>
        {loadingAgg ? (
          <p className="loading">Atualizando agregações…</p>
        ) : !year || !agg.length ? (
          <p>Nenhum dado disponível para este ano.</p>
        ) : (
          <div className="months">
            {months.map((name, index) => (
              <div className="month" key={name}>
                <h3>{name}</h3>
                {categories.map((category) => {
                  const item = agg.find(
                    (x) =>
                      Number(x.month_number) === index + 1 &&
                      x.category === category,
                  );
                  return (
                    <div className="aggregate-row" key={category}>
                      <span>{category}</span>
                      <small>
                        {money.format(item?.total || 0)} (
                        {Number(item?.count || 0).toLocaleString()} registros)
                      </small>
                    </div>
                  );
                })}
              </div>
            ))}
          </div>
        )}
      </section>
      <section className="panel">
        <div className="title">
          <h2>Transações</h2>
          <div className="filters">
            <label>
              Ano:{" "}
              <select
                value={transactionYear}
                onChange={(e) => setTransactionYear(e.target.value)}
              >
                {years.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Mês:{" "}
              <select
                value={transactionMonth}
                onChange={(e) => setTransactionMonth(e.target.value)}
              >
                <option value="">Todos</option>
                {months.map((name, index) => (
                  <option key={name} value={index + 1}>
                    {name}
                  </option>
                ))}
              </select>
            </label>
          </div>
          <span>
            {transactionYear
              ? "Filtro ativo"
              : `${total.toLocaleString()} registros`}
          </span>
        </div>
        <div className="table">
          <table>
            <thead>
              <tr>
                <th>Data</th>
                <th>Categoria</th>
                <th>Valor</th>
                <th>Origem</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.id}>
                  <td>{new Date(r.occurred_at).toLocaleString("pt-BR")}</td>
                  <td>{r.category}</td>
                  <td>{money.format(r.amount)}</td>
                  <td>{r.source}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {!rows.length && <p>Nenhum registro encontrado.</p>}
        </div>
        <nav>
          <button
            disabled={!cursorIndex}
            onClick={() => setCursorIndex(cursorIndex - 1)}
          >
            Anterior
          </button>
          <span>Lote {cursorIndex + 1}</span>
          <button
            disabled={!nextCursor}
            onClick={() => {
              setCursors([...cursors, nextCursor]);
              setCursorIndex(cursorIndex + 1);
            }}
          >
            Próxima
          </button>
        </nav>
      </section>
    </main>
  );
}
createRoot(document.getElementById("root")).render(<App />);
