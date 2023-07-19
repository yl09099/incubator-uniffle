const fileUtil = require('./fileutils')

// Destination folder
const staticDirectory = '../resources/static'
// Delete
fileUtil.deleteFolder(staticDirectory)
// Copy
fileUtil.copyFolder('./dist', staticDirectory)
console.log('File copy successful!')
