const { initHeaderRow, getSheetInfo } = require('./common/google');

(async () => {
  const info = getSheetInfo();
  const result = await initHeaderRow();
  console.log('Master sheet ready:', info);
  console.log(result.created ? 'Header row created.' : 'Header already existed.');
})().catch(err => {
  console.error(err);
  process.exit(1);
});
