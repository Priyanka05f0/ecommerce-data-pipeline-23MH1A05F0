def test_imports():
    import scripts.pipeline_orchestrator
    import scripts.ingestion.ingest_to_staging
    import scripts.quality_checks.validate_data
    import scripts.transformation.staging_to_production
    import scripts.transformation.load_warehouse
