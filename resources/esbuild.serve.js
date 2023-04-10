import * as esbuild from 'esbuild'

let ctx = await esbuild.context({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outdir: 'res',
})

await ctx.watch()

let { host, port } = await ctx.serve({
    servedir: 'res',
    
})

console.log(host, port);