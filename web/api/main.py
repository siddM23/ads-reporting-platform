if __name__ == "__main__":
    import uvicorn
    uvicorn.run("entry:app", host="0.0.0.0", port=8000, reload=True)