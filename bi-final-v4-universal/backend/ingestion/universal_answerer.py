"""
ingestion/universal_answerer.py
Answers questions from ANY Tableau DataFrame using pandas alone.
Works with SchemaProfile — adapts to every dashboard type.
"""
from __future__ import annotations
import re, logging
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from backend.ingestion.schema_analyser import SchemaProfile

log = logging.getLogger("universal_answerer")

@dataclass
class Answer:
    reply:      str
    data:       Optional[Any]
    chart:      Optional[Dict]
    confidence: float = 1.0
    method:     str   = "rules"
    row_count:  int   = 0

    def to_dict(self) -> Dict:
        data_out = None
        if isinstance(self.data, pd.DataFrame):
            data_out = self.data.head(200).to_dict("records")
        elif isinstance(self.data, dict):
            data_out = self.data
        return {"reply": self.reply, "data": data_out, "chart": self.chart,
                "confidence": self.confidence, "method": self.method, "row_count": self.row_count}

INTENT_PATTERNS: List[Tuple[List[str], str]] = [
    (["breach","exceed","fail","violated","out of","below threshold","above threshold"], "breach"),
    (["red","amber","green","rag","status","failing","passing","compliant","flag"],      "rag_filter"),
    (["trend","over time","monthly","history","last 6","last 12","period","change over"],"trend"),
    (["highest","best","top","maximum","max","most","largest"],                          "highest"),
    (["lowest","worst","bottom","minimum","min","least","smallest"],                     "lowest"),
    (["average","avg","mean","typical"],                                                 "average"),
    (["total","sum","count","how many","number of"],                                     "count_sum"),
    (["variance","change","delta","difference","improved","deteriorated"],               "variance"),
    (["breakdown","distribution","split","proportion","share","percentage"],             "distribution"),
    (["compare","vs","versus","contrast","against","side by side"],                      "compare"),
    (["rank","ranking","list all","show all","top 5","top 10"],                          "ranking"),
    (["outlier","anomaly","unusual","spike","drop","unexpected"],                        "outliers"),
    (["correlat","relationship between","link between"],                                 "correlation"),
    (["summary","overview","give me","describe","what is this"],                         "summary"),
]

_COLORS = ["#6366F1","#F43F5E","#10B981","#F59E0B","#22D3EE","#8B5CF6","#9CA3AF"]

class UniversalAnswerer:
    def __init__(self, df: pd.DataFrame, profile: SchemaProfile):
        self.df = df.copy()
        self.profile = profile
        self._dispatch: Dict[str, Callable] = {
            "breach":       self._breach,
            "rag_filter":   self._rag_filter,
            "trend":        self._trend,
            "highest":      self._highest,
            "lowest":       self._lowest,
            "average":      self._average,
            "count_sum":    self._count_sum,
            "variance":     self._variance,
            "distribution": self._distribution,
            "compare":      self._compare,
            "ranking":      self._ranking,
            "outliers":     self._outliers,
            "correlation":  self._correlation,
            "summary":      self._summary,
        }

    def answer(self, question: str) -> Answer:
        intent  = self._intent(question)
        handler = self._dispatch.get(intent, self._summary)
        log.info(f"Intent={intent} | Q={question[:60]}")
        try:
            ans = handler(question)
            ans.method = "rules"
            return ans
        except Exception as e:
            log.error(f"Handler {intent} failed: {e}")
            return Answer(reply=f"Could not compute: {e}", data=None, chart=None, confidence=0.3)

    def can_answer(self, question: str) -> bool:
        return not self.df.empty

    # ── Intent detection ──────────────────────────────────────────────────
    def _intent(self, q: str) -> str:
        ql = q.lower()
        for kws, intent in INTENT_PATTERNS:
            if any(kw in ql for kw in kws):
                return intent
        return "summary"

    # ── Column helpers ────────────────────────────────────────────────────
    def _ms(self):  return [c for c in self.profile.measures   if c in self.df.columns]
    def _ds(self):  return [c for c in self.profile.dimensions if c in self.df.columns]
    def _tc(self):  return next((c for c in self.profile.time_cols   if c in self.df.columns), None)
    def _sc(self):  return next((c for c in self.profile.status_cols if c in self.df.columns), None)
    def _pm(self):  return self._ms()[0] if self._ms() else None
    def _pd(self):  return self._ds()[0] if self._ds() else None

    def _term(self):
        return {"kri":"KRIs","scorecard":"metrics","compliance":"controls",
                "sales":"deals","ops":"incidents"}.get(self.profile.dashboard_type,"records")

    def _fmt(self, v):
        if isinstance(v, float):
            return f"{v:.2%}" if 0 <= v <= 1 else f"{v:,.3f}"
        return str(v)

    def _n(self, q: str, default=5) -> int:
        nums = re.findall(r"\b(\d+)\b", q)
        return int(nums[0]) if nums else default

    def _dim_from_q(self, q: str):
        ql = q.lower()
        for d in self._ds():
            if d.lower() in ql: return d
        return self._pd()

    def _msr_from_q(self, q: str):
        ql = q.lower()
        for m in self._ms():
            if m.lower() in ql: return m
        return self._pm()

    # ── Chart builders ────────────────────────────────────────────────────
    def _tbl(self, df: pd.DataFrame) -> Dict:
        return {"chart_type":"table","title":f"Results ({len(df)} rows)",
                "subtitle":self.profile.source_name,
                "columns":df.columns.tolist(),"rows":df.head(50).values.tolist()}

    def _bar(self, labels, values, title) -> Dict:
        return {"chart_type":"bar","title":title,"subtitle":self.profile.source_name,
                "labels":[str(l) for l in labels],
                "datasets":[{"label":title,"data":[float(v) if v==v else 0 for v in values],"color":_COLORS[0]}]}

    def _line(self, labels, ds_map: Dict[str, list], title) -> Dict:
        return {"chart_type":"line","title":title,"subtitle":self.profile.source_name,
                "labels":[str(l) for l in labels],
                "datasets":[{"label":n,"data":[float(v) if v==v else 0 for v in vals],"color":_COLORS[i%len(_COLORS)]}
                            for i,(n,vals) in enumerate(ds_map.items())]}

    def _pie(self, labels, values, title, dtype="doughnut") -> Dict:
        return {"chart_type":dtype,"title":title,"subtitle":self.profile.source_name,
                "labels":[str(l) for l in labels],
                "data":[float(v) if v==v else 0 for v in values],
                "colors":_COLORS[:len(labels)]}

    def _kpi(self, kpis: list) -> Dict:
        return {"chart_type":"kpi","title":"Key Metrics","subtitle":self.profile.source_name,"kpis":kpis}

    # ── Handlers ──────────────────────────────────────────────────────────
    def _breach(self, q: str) -> Answer:
        result = pd.DataFrame()
        notes  = []
        for kp in self.profile.kpi_patterns:
            if kp.threshold_col and kp.threshold_col in self.df.columns and kp.value_col in self.df.columns:
                v, t = self.df[kp.value_col], self.df[kp.threshold_col]
                mask = (v < t) if v.mean() < t.mean() else (v > t)
                br   = self.df[mask].copy()
                if not br.empty:
                    result = br
                    notes.append(f"{len(br)} breach(es) on {kp.kpi_name}")
        if result.empty and self._sc():
            sc = self._sc()
            bad = {s for s in self.df[sc].dropna().unique() if str(s).lower() in {"red","fail","breach","critical","no","false"}}
            if bad:
                result = self.df[self.df[sc].isin(bad)]
                notes.append(f"{len(result)} {self._term()} with {sc} in {bad}")
        if result.empty:
            return Answer(reply=f"No breaches detected. All {self._term()} appear within thresholds.", data=None, chart=None)
        return Answer(reply=" | ".join(notes), data=result, chart=self._tbl(result),
                      confidence=1.0, row_count=len(result))

    def _rag_filter(self, q: str) -> Answer:
        sc = self._sc()
        if not sc:
            return Answer(reply="No status column found.", data=None, chart=None, confidence=0.5)
        ql = q.lower()
        for tv in ["red","amber","green","fail","pass","breach","compliant","critical","high","medium","low"]:
            if tv in ql:
                f = self.df[self.df[sc].astype(str).str.lower().str.contains(tv, na=False)]
                if not f.empty:
                    return Answer(reply=f"{len(f)} {self._term()} have {sc} matching '{tv}'.",
                                  data=f, chart=self._tbl(f), confidence=1.0, row_count=len(f))
        return self._distribution(q)

    def _trend(self, q: str) -> Answer:
        tc = self._tc()
        m  = self._msr_from_q(q)
        if not tc:
            return Answer(reply="No time column found.", data=None, chart=None, confidence=0.4)
        if not m:
            return Answer(reply="No numeric measure for trend.", data=None, chart=None, confidence=0.4)
        dim = self._dim_from_q(q)
        if dim and dim != tc and dim in self.df.columns:
            pivot = self.df[[tc,dim,m]].dropna().groupby([tc,dim])[m].mean().unstack(dim)
            labels  = [str(i) for i in pivot.index]
            ds_map  = {str(c): pivot[c].tolist() for c in pivot.columns}
            return Answer(reply=f"Trend of {m} by {dim} over {tc} ({len(pivot)} periods).",
                          data=pivot.reset_index(), chart=self._line(labels, ds_map, f"{m} by {dim}"),
                          confidence=1.0, row_count=len(pivot))
        trend = self.df[[tc,m]].dropna().groupby(tc)[m].mean().reset_index().sort_values(tc)
        labels  = trend[tc].astype(str).tolist()
        ds_map  = {m: trend[m].tolist()}
        chg     = trend[m].iloc[-1] - trend[m].iloc[0] if len(trend)>1 else 0
        return Answer(reply=f"Trend of {m}: {len(trend)} periods. Change: {self._fmt(chg)}.",
                      data=trend, chart=self._line(labels, ds_map, f"Trend: {m}"),
                      confidence=1.0, row_count=len(trend))

    def _highest(self, q: str) -> Answer:
        n  = self._n(q)
        m  = self._msr_from_q(q)
        if not m:
            return Answer(reply="No numeric column to rank.", data=None, chart=None, confidence=0.4)
        top = self.df.nlargest(n, m)
        dim = self._pd()
        if dim and dim in top.columns:
            return Answer(reply=f"Top {n} by {m}: max={self._fmt(top[m].max())}",
                          data=top, chart=self._bar(top[dim].tolist(), top[m].tolist(), f"Top {n}: {m}"),
                          confidence=1.0, row_count=len(top))
        return Answer(reply=f"Top {n} by {m}: max={self._fmt(top[m].max())}",
                      data=top, chart=self._tbl(top), confidence=1.0, row_count=len(top))

    def _lowest(self, q: str) -> Answer:
        n  = self._n(q)
        m  = self._msr_from_q(q)
        if not m:
            return Answer(reply="No numeric column to rank.", data=None, chart=None, confidence=0.4)
        bot = self.df.nsmallest(n, m)
        dim = self._pd()
        if dim and dim in bot.columns:
            return Answer(reply=f"Bottom {n} by {m}: min={self._fmt(bot[m].min())}",
                          data=bot, chart=self._bar(bot[dim].tolist(), bot[m].tolist(), f"Bottom {n}: {m}"),
                          confidence=1.0, row_count=len(bot))
        return Answer(reply=f"Bottom {n} by {m}: min={self._fmt(bot[m].min())}",
                      data=bot, chart=self._tbl(bot), confidence=1.0, row_count=len(bot))

    def _average(self, q: str) -> Answer:
        m   = self._msr_from_q(q)
        dim = self._dim_from_q(q)
        if not m:
            avgs = {k: round(float(self.df[k].mean()), 4) for k in self._ms()}
            kpis = [{"label":k,"value":self._fmt(v),"subtitle":"avg","color":"blue"} for k,v in avgs.items()]
            return Answer(reply=f"Averages: {avgs}", data=avgs, chart=self._kpi(kpis), confidence=1.0)
        if dim and dim in self.df.columns:
            grp = self.df.groupby(dim)[m].mean().round(4).reset_index()
            grp.columns = [dim, f"avg_{m}"]
            return Answer(reply=f"Avg {m} by {dim}: {grp.set_index(dim)[f'avg_{m}'].to_dict()}",
                          data=grp, chart=self._bar(grp[dim].tolist(), grp[f"avg_{m}"].tolist(), f"Avg {m} by {dim}"),
                          confidence=1.0, row_count=len(grp))
        avg = float(self.df[m].mean())
        return Answer(reply=f"Average {m} = {self._fmt(avg)} (n={len(self.df)})",
                      data={"avg":avg,"col":m},
                      chart=self._kpi([{"label":f"Avg {m}","value":self._fmt(avg),"subtitle":f"n={len(self.df)}","color":"blue"}]),
                      confidence=1.0)

    def _count_sum(self, q: str) -> Answer:
        ql = q.lower()
        m  = self._msr_from_q(q) if any(w in ql for w in ["sum","total"]) else None
        dim = self._dim_from_q(q)
        sc  = self._sc()
        if ("how many" in ql or "count" in ql) and sc:
            vc = self.df[sc].value_counts().reset_index()
            vc.columns = [sc,"count"]
            return Answer(reply=f"Count by {sc}: " + ", ".join(f"{r[sc]}={r['count']}" for _,r in vc.iterrows()),
                          data=vc, chart=self._pie(vc[sc].tolist(), vc["count"].tolist(), f"Count by {sc}"),
                          confidence=1.0, row_count=len(self.df))
        if dim and dim in self.df.columns and not m:
            vc = self.df[dim].value_counts().reset_index()
            vc.columns = [dim,"count"]
            return Answer(reply=f"{len(self.df)} total. By {dim}: " + ", ".join(f"{r[dim]}={r['count']}" for _,r in vc.head(5).iterrows()),
                          data=vc, chart=self._bar(vc[dim].tolist(), vc["count"].tolist(), f"Count by {dim}"),
                          confidence=1.0, row_count=len(self.df))
        if m:
            total = float(self.df[m].sum())
            return Answer(reply=f"Total {m} = {self._fmt(total)}",
                          data={"total":total,"col":m},
                          chart=self._kpi([{"label":f"Total {m}","value":self._fmt(total),"subtitle":f"n={len(self.df)}","color":"blue"}]),
                          confidence=1.0)
        return Answer(reply=f"Total: {len(self.df)} {self._term()}.",
                      data={"count":len(self.df)},
                      chart=self._kpi([{"label":self._term().title(),"value":str(len(self.df)),"subtitle":self.profile.source_name,"color":"blue"}]),
                      confidence=1.0)

    def _variance(self, q: str) -> Answer:
        var_kps = [kp for kp in self.profile.kpi_patterns if kp.previous_col and kp.previous_col in self.df.columns]
        if var_kps:
            kp  = var_kps[0]
            v,p = self.df[kp.value_col], self.df[kp.previous_col]
            delta   = (v - p).round(4)
            pct_chg = ((delta / p.abs()) * 100).round(2)
            result  = self.df.copy()
            result["_delta"]   = delta
            result["_pct_chg"] = pct_chg
            result = result.sort_values("_pct_chg")
            return Answer(reply=f"Variance {kp.value_col} vs {kp.previous_col}. Avg change: {self._fmt(float(delta.mean()))} ({float(pct_chg.mean()):.1f}%)",
                          data=result, chart=self._bar(list(range(len(result))), result["_pct_chg"].tolist(), "% Change"),
                          confidence=1.0, row_count=len(result))
        m = self._msr_from_q(q)
        if m:
            return Answer(reply=f"Std dev of {m} = {float(self.df[m].std()):.4f}",
                          data={"std":float(self.df[m].std())}, chart=None, confidence=0.7)
        return Answer(reply="No previous-period columns found.", data=None, chart=None, confidence=0.3)

    def _distribution(self, q: str) -> Answer:
        sc  = self._sc()
        dim = self._dim_from_q(q)
        col = sc if sc and "status" in q.lower() else (dim or sc)
        if not col or col not in self.df.columns:
            ds = self._ds()
            col = ds[0] if ds else None
        if not col:
            return self._summary(q)
        vc  = self.df[col].value_counts().reset_index()
        vc.columns = [col,"count"]
        vc["pct"] = (vc["count"] / len(self.df) * 100).round(1)
        cm = {"red":"#F43F5E","fail":"#F43F5E","amber":"#F59E0B","green":"#10B981","pass":"#10B981"}
        colors = [cm.get(str(v).lower(), "#6366F1") for v in vc[col]]
        chart  = {"chart_type":"doughnut","title":f"Distribution by {col}","subtitle":self.profile.source_name,
                  "labels":vc[col].astype(str).tolist(),"data":vc["count"].tolist(),"colors":colors}
        return Answer(reply=f"Distribution of {col}: " + ", ".join(f"{r[col]}={r['count']} ({r['pct']}%)" for _,r in vc.iterrows()),
                      data=vc, chart=chart, confidence=1.0, row_count=len(self.df))

    def _compare(self, q: str) -> Answer:
        dim  = self._dim_from_q(q)
        ms   = [self._msr_from_q(q)] if self._msr_from_q(q) else self._ms()[:2]
        if not dim or dim not in self.df.columns:
            return Answer(reply="No dimension to compare across.", data=None, chart=None, confidence=0.4)
        grp  = self.df.groupby(dim)[ms].mean().round(4).reset_index()
        chart = {"chart_type":"bar","title":f"Comparison by {dim}","subtitle":self.profile.source_name,
                 "labels":grp[dim].astype(str).tolist(),
                 "datasets":[{"label":c,"data":grp[c].tolist(),"color":_COLORS[i]} for i,c in enumerate(ms)]}
        return Answer(reply=f"Comparison of {ms} by {dim} ({len(grp)} groups).",
                      data=grp, chart=chart, confidence=1.0, row_count=len(grp))

    def _ranking(self, q: str) -> Answer:
        n  = self._n(q, 10)
        m  = self._msr_from_q(q)
        if not m:
            return Answer(reply=f"First {n} records.", data=self.df.head(n),
                          chart=self._tbl(self.df.head(n)), confidence=0.7, row_count=n)
        ranked = self.df.sort_values(m, ascending=False).head(n)
        dim    = self._pd()
        if dim and dim in ranked.columns:
            return Answer(reply=f"Top {n} by {m}: max={self._fmt(float(ranked[m].max()))}",
                          data=ranked, chart=self._bar(ranked[dim].astype(str).tolist(), ranked[m].tolist(), f"Ranked by {m}"),
                          confidence=1.0, row_count=len(ranked))
        return Answer(reply=f"Top {n} by {m}.", data=ranked, chart=self._tbl(ranked), confidence=1.0, row_count=len(ranked))

    def _outliers(self, q: str) -> Answer:
        m = self._msr_from_q(q) or self._pm()
        if not m:
            return Answer(reply="No numeric column.", data=None, chart=None, confidence=0.4)
        col  = self.df[m].dropna()
        q1,q3 = col.quantile(0.25), col.quantile(0.75)
        iqr   = q3 - q1
        lo,hi = q1 - 1.5*iqr, q3 + 1.5*iqr
        out   = self.df[(self.df[m] < lo) | (self.df[m] > hi)]
        if out.empty:
            return Answer(reply=f"No outliers in {m} (IQR method). Range: {self._fmt(lo)}–{self._fmt(hi)}",
                          data=None, chart=None, confidence=1.0)
        return Answer(reply=f"{len(out)} outliers in {m} (range: {self._fmt(lo)}–{self._fmt(hi)})",
                      data=out, chart=self._tbl(out), confidence=1.0, row_count=len(out))

    def _correlation(self, q: str) -> Answer:
        ms = self._ms()
        if len(ms) < 2:
            return Answer(reply="Need ≥2 numeric columns.", data=None, chart=None, confidence=0.4)
        corr  = self.df[ms].corr().round(3)
        strong = [f"{ms[i]}↔{ms[j]}: {corr.iloc[i,j]:.2f}"
                  for i in range(len(ms)) for j in range(i+1,len(ms)) if abs(corr.iloc[i,j])>0.7]
        return Answer(reply="Strong correlations: " + ("; ".join(strong) if strong else "None detected (>0.7)."),
                      data=corr, chart=self._tbl(corr.reset_index()), confidence=1.0)

    def _summary(self, q: str) -> Answer:
        p    = self.profile
        sc   = self._sc()
        kpis = []
        if sc and sc in self.df.columns:
            cm = {"red":"red","fail":"red","amber":"amber","green":"green","pass":"green"}
            for val, cnt in self.df[sc].value_counts().items():
                kpis.append({"label":str(val),"value":str(cnt),"subtitle":f"{cnt/len(self.df)*100:.0f}%",
                             "color":cm.get(str(val).lower(),"blue")})
        for m in self._ms()[:4]:
            st = self.df[m].describe()
            kpis.append({"label":m,"value":self._fmt(float(self.df[m].mean())),
                         "subtitle":f"{self._fmt(float(st['min']))}–{self._fmt(float(st['max']))}","color":"blue"})
        reply = (f"{p.source_name}: {p.total_rows} rows | "
                 f"Type: {p.dashboard_type} | "
                 f"Dims: {p.dimensions[:3]} | "
                 f"Measures: {p.measures[:3]}")
        if sc and sc in self.df.columns:
            reply += " | " + ", ".join(f"{k}={v}" for k,v in self.df[sc].value_counts().items())
        return Answer(reply=reply, data=self.df.head(20),
                      chart=self._kpi(kpis) if kpis else self._tbl(self.df.head(10)),
                      confidence=1.0, row_count=len(self.df))
