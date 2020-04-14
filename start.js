const { pipe, map, max, reduce, flatten, keys, has, groupBy, prop, is, mergeAll, toPairs, isEmpty } = require('ramda');
const fs = require('fs');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const dados = require('./dadosmunicipios.json');

const agruparPorLocalidade =  groupBy(prop('localidade'));
const prepararVariaveis = variavel => {
  return variavel.res.map(municipio => {
    const maxKey = reduce(max, '0', keys(municipio.res));
    const tem2018 = has('2018', municipio.res);
    const ano = tem2018 ? '2018' : maxKey;
    const valor = municipio.res[ano];
    return {
      localidade: municipio.localidade,
      [variavel.id]: valor
    };
  })
};
const dividirPor = divisor => valor => (divisor) ? (valor / divisor).toFixed(5) : 0;

const mediaIDH = 0.659;
const codigoIDH = '30255';
const codigoPopulacao = '29171';
const codigoMatriculasEnsFund = '5908';
const codigoMatriculasEnsMed = '5913';
const codigoDocentesEnsFund = '5929';
const codigoDocentesEnsMed = '5934';
const codigoNumEstabelecimentoEnsFund = '5950';
const codigoNumEstabelecimentoEnsMed = '5955';

const tratarDados = (dados) => {
  const idh = dados[codigoIDH];
  const populacao = dados[codigoPopulacao]
  const matriculasEnsFund = dados[codigoMatriculasEnsFund];
  const matriculasEnsMed = dados[codigoMatriculasEnsMed];
  const docentesEnsFund = dados[codigoDocentesEnsFund];
  const docentesEnsMed = dados[codigoDocentesEnsMed];
  const numEstabelecimentoEnsFund = dados[codigoNumEstabelecimentoEnsFund];
  const numEstabelecimentoEnsMed = dados[codigoNumEstabelecimentoEnsMed];
  const dividirPorPopulacao = dividirPor(populacao);

  const porPop = {
    matriculaEnsFundPorPop: dividirPorPopulacao(matriculasEnsFund),
    matriculasEnsMedPorPop: dividirPorPopulacao(matriculasEnsMed),
    docentesEnsFundPorPop: dividirPorPopulacao(docentesEnsFund),
    docentesEnsMedPorPop: dividirPorPopulacao(docentesEnsMed),
    numEstabelecimentoEnsFundPorPop: dividirPorPopulacao(numEstabelecimentoEnsFund),
    numEstabelecimentoEnsMedPorPop: dividirPorPopulacao(numEstabelecimentoEnsMed),
  };
  return {
    ...dados,
    [codigoIDH]: is(String, idh) && (idh.includes('-') || isEmpty(idh.trim())) ? mediaIDH : idh,
    ...porPop,
  }
}

const prepararDados = pipe(
  map(prepararVariaveis),
  flatten,
  agruparPorLocalidade,
  toPairs,
  map(pipe(prop(1), mergeAll)),
  map(tratarDados),
);

const csvWriter = createCsvWriter({
    path: './dadosmunicipios.csv',
    header: [
        {id: 'localidade', title: 'municipio'},
        {id: codigoMatriculasEnsFund, title: 'matfund'},
        {id: 'matriculaEnsFundPorPop', title: 'matfundpop'},
        {id: codigoMatriculasEnsMed, title: 'matmed'},
        {id: 'matriculasEnsMedPorPop', title: 'matmedpop'},
        {id: codigoDocentesEnsFund, title: 'docfund'},
        {id: 'docentesEnsFundPorPop', title: 'docfundpop'},
        {id: codigoDocentesEnsMed, title: 'docmed'},
        {id: 'docentesEnsMedPorPop', title: 'docmedpop'},
        {id: codigoNumEstabelecimentoEnsFund, title: 'nestabfund'},
        {id: 'numEstabelecimentoEnsFundPorPop', title: 'nestabfundpop'},
        {id: codigoNumEstabelecimentoEnsMed, title: 'nestabmed'},
        {id: 'numEstabelecimentoEnsMedPorPop', title: 'nestabmedpop'},
        {id: codigoPopulacao, title: 'populacao'},
        {id: codigoIDH, title: 'idh'},
        {id: '47001', title: 'pib'},
    ]
});

const dadosPreparados = prepararDados(dados);
csvWriter.writeRecords(dadosPreparados).then(() => {
  console.log('sucesso');
});