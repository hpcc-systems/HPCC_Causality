IMPORT $.^.^ AS HC;
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

malesPS := prob.SubSpace('gender=male', prob.PS);

summary2 := prob.Summary(malesPS);

OUTPUT(summary2, NAMED('DatasetSummaryMales'));

femalesPS := prob.SubSpace('gender=female', prob.PS);

summary3 := prob.Summary(femalesPS);

OUTPUT(summary3, NAMED('DatasetSummaryFemales'));

tests := ['E(height | gender=male)',
          'E(height | gender=female)'];

results := prob.Query(tests);

OUTPUT(results, NAMED('ProbResults'));

//query := 'P(genhealth in [4,5] | state)';
query := 'P(age)';
queries := [
            //'E(weight | height, age, gender=male)'
            //'E(genhealth | state, controlFor(age, income))'
            //'E(genhealth | state)'
            //'P(genhealth = 5-excellent |age)',
            //'P(genhealth in [3-good, 4-verygood, 5-excellent] | age, income)'
            //'P(age > 65, gender=male, married=no | genhealth)'
            //'P(genhealth in [4-verygood, 5-excellent] | state, controlFor(age, income))'
            //'CORRELATION(gender, income, genhealth, physicalactivity, age, diabetes)',
            //'DEPENDENCE(gender, income, genhealth, physicalactivity, age, diabetes)'
            //'CMODEL(gender, age, income, height, weight, physicalactivity, education, genhealth)',
            //'CMODEL(gender, age, income, height, weight, physicalactivity, education, genhealth | $power=8, $sensitivity=6, $depth=3)'
            //'P(age | genhealth in [1-poor, 2-fair])',
            //'P(weight | height, gender=male)'
            'P(age)',
            'P(weight | gender=male)',
            'P(weight | gender = female)'
            ];

pr := viz.parseQuery(query, prob.PS);
//OUTPUT(pr);
gr := viz.getGrid(pr, prob.PS);
OUTPUT(gr, NAMED('Grid'));

cd := viz.GetDataGrid(query, prob.PS);
//OUTPUT(cd);
viz.Plot(queries, prob.PS);