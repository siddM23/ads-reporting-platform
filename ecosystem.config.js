module.exports = {
    apps: [
        {
            name: "frontend",
            cwd: "./frontend",
            script: "npm",
            args: "start",
            env: {
                NODE_ENV: "production",
                PORT: 3000
            }
        },
        {
            name: "backend",
            cwd: "./frontend/api",
            script: "main.py",
            interpreter: "python3",
            env: {
                PORT: 8000
            }
        }
    ]
};
