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

dst := ds[1..10000];

hc.ToAnyField(dst, dsf, ,'gender,height,weight,income,age,veteran,genhealth');

prob := Probability(dsf, dsf_fields);
tests := DATASET([{1, 'P(height <= 66)'}
                ], nlQuery);

results := prob.Query(tests);

query := 'E(height | weight)';

g := viz.getDataGrid(query, prob.PS);
OUTPUT(g);
viz.Plot(['P(height)','P(weight)'], prob.PS);