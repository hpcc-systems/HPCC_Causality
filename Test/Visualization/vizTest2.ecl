IMPORT HPCC_causality AS HC;
IMPORT HC.Types;

Probability := HC.Probability;
viz := HC.viz;
ProbSpec := Types.ProbSpec;
ProbQuery := Types.ProbQuery;
nlQuery := Types.nlQuery;

fmt :=  RECORD
    UNSIGNED age;
    STRING gender;
    UNSIGNED weight;
    UNSIGNED height;
    UNSIGNED ageGroup;
    STRING genhealth;
    STRING asthma_ever;
    STRING asthma;
    STRING skincancer;
    STRING othercancer;
    STRING copd;
    STRING arthritis;
    STRING depression;
    STRING kidneydis;
    STRING diabetes;
    STRING maritaldetail;
    STRING married;
    STRING education;
    STRING veteran;
    UNSIGNED income;
    STRING state;
    STRING childcnt;
    STRING sleephours;
    STRING employment;
    STRING smokertype;
    STRING physicalactivity;
    STRING insurance;
    STRING checkup;
    STRING nohospitalcost;
    UNSIGNED bmi;
    STRING bmicat;
    UNSIGNED drinks;
END;                               

ds0 := DATASET('llcp.csv', fmt, CSV(HEADING(1)));

hc.AddID(ds0, ds);

dst := ds;

//hc.ToAnyField(dst, dsf, ,'gender,height,weight,income,age,veteran,genhealth,state');
hc.ToAnyField(dst, dsf);

categoricals := ['state', 'smokertype'];

prob := Probability(dsf, dsf_fields, categoricals);

summary := prob.Summary();

OUTPUT(summary, NAMED('DatasetSummary'));

tests := DATASET([{1, 'P(height <= 66)'}
                ], nlQuery);

results := prob.Query(tests);

//query := 'P(genhealth in [4,5] | state)';
query := 'CModel(height, weight, income, genhealth | $sensitivity=6)';
queries := ['P(genhealth in [4,5] | income)', 'CModel(gender,height, weight, age | $power=5.0,$sensitivity=10.0, $depth=2)'];

pr := viz.parseQuery(query, prob.PS);
//OUTPUT(pr);
vars := viz.getVarNames(pr);
//OUTPUT(vars);
g := viz.GetHeatmapGrid(pr, prob.PS);
//OUTPUT(g);
//fg := viz.fillHeatmapGrid(g, ['weight', 'height'], 'dep', prob.PS);
//OUTPUT(fg);

cd := viz.GetDataGrid(query, prob.PS);
//OUTPUT(cd);
viz.Plot(queries, prob.PS);