import dlt

if __name__ == "__main__":
    p = dlt.attach("dlt_github_pipeline")
    info = p.normalize()
    print(info)
