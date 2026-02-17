module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 'latest',
    sourceType: 'module',
  },
  env: {
    es2022: true,
    node: true,
    browser: true,
  },
  ignorePatterns: [
    '**/dist/**',
    '**/node_modules/**',
    '**/__pycache__/**',
    '**/*.pyc',
  ],
};
