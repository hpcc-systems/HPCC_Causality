import { Library, Runtime, Inspector } from "@observablehq/runtime";
import { Workunit, Result } from "@hpcc-js/comms";
import { compile } from "@hpcc-js/observablehq-compiler";
import { scopedLogger } from "@hpcc-js/util";

import "@hpcc-js/observablehq-compiler/dist/index.esm.css";

const logger = scopedLogger("resources/src/index.ts");

const regex = /W\d{8}-\d{6}-?\d?/;
const isLocalTesting = location.href.indexOf("localhost:8000") > 0;
const baseUrl = isLocalTesting ? "http://localhost:8010/" : "";
const __hpcc_index_html = "__hpcc_index_html";

interface PlotMeta {
    dataname: string;
    qtype: string;
    dims: string;
    [label: string]: string | number;
}

class Plot {

    meta?: PlotMeta;
    data?: Array<string | number>[];

    constructor(protected main: Main, protected metaName: string) {
    }

    async fetchData() {
        return this.main.results.get(this.metaName)?.fetchRows().then((plotMeta: PlotMeta[]) => {
            this.meta = plotMeta[0];
            return this.main.results.get(this.meta.dataname)?.fetchRows() ?? Promise.resolve([]);
        }).then(data => {
            this.data = data;
            return data;
        });
    }
}

class Main {

    wu: Workunit;
    results: Map<string, Result> = new Map();
    plots: Map<string, Plot> = new Map();
    meta: any[];

    constructor(protected wuid: string) {
        logger.debug(`Wuid:  ${this.wuid})`);
        this.wu = Workunit.attach({ baseUrl }, wuid);
    }

    async fetchPlots() {
        await this.wu.watchUntilComplete().then(() => {
            return this.wu.fetchResults();
        }).then(results => {
            results.forEach(result => {
                this.results.set(result.Name, result);
            });
            return this.results.get(__hpcc_index_html);
        }).then(result => {
            return result?.fetchRows() ?? [];
        }).then(meta => {
            this.meta = meta;
            return Promise.all(meta.map(row => {
                const plot = new Plot(this, row.name);
                this.plots.set(row.name, plot);
                return plot.fetchData();
            }));
        }).catch(e => logger.error(e));
    }

    globals() {
        const globals = {
            __hpcc_index_html: this.meta,
            results: {}
        };
        this.plots.forEach((plot, name) => {
            globals.results[name] = [plot.meta];
            globals.results[plot.meta?.dataname ?? ""] = plot.data;
        });
        return globals;
    }
}

export async function compileViz(): Promise<any> {
    const matches = location.href.match(regex);
    const wuid = matches?.[0]!;

    const main = new Main(wuid);
    await main.fetchPlots();

    const placeholder = document.getElementById("placeholder")!;

    const nb = await fetch("./index.eclnb").then(r => r.json());
    nb.nodes = nb.nodes.filter(node => node.mode !== "ecl");

    const compiledNB = await compile(nb);

    const runtime = new Runtime(Object.assign(new Library, main.globals()));
    compiledNB(runtime, _name => {
        const div = document.createElement("div");
        placeholder.appendChild(div);

        return new Inspector(div);
    });
}

new EventSource('/esbuild').addEventListener('change', () => location.reload())
