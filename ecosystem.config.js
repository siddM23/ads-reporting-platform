module.exports = {
    apps: [
        {
            name: "frontend",
            cwd: "./web",
            script: "npm",
            args: "start",
            env: {
                NODE_ENV: "production",
                PORT: 3000
            }
        },
        {
            name: "backend",
            cwd: "./web/api",
            script: "entry.py",
            interpreter: "python3",
            env: {
                PORT: 8000
            }
        }
    ]
};
