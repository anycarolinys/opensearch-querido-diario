""" def try_process_gazette_file(
    gazette: Dict,
    territories: Iterable[Dict[str, Any]],
    database: DatabaseInterface,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
) -> Dict:
    # Do all the work to extract the content from the gazette files
    
    logging.debug(f"Processing gazette {gazette['file_path']}")
    gazette_file = download_gazette_file(gazette, storage)
    gazette["source_text"] = try_to_extract_content(gazette_file, text_extractor)
    gazette["url"] = define_file_url(gazette["file_path"])
    gazette_txt_path = define_gazette_txt_path(gazette)
    gazette["file_raw_txt"] = define_file_url(gazette_txt_path)
    upload_raw_text(gazette_txt_path, gazette["source_text"], storage)
    delete_gazette_files(gazette_file)

    document_ids = []
    if gazette_type_is_aggregated(gazette):
        segmenter = get_segmenter(gazette["territory_id"], territories)
        territory_segments = segmenter.get_gazette_segments(gazette)

        for segment in territory_segments:
            segment_txt_path = define_segment_txt_path(segment)
            segment["file_raw_txt"] = define_file_url(segment_txt_path)
            upload_raw_text(segment_txt_path, segment["source_text"], storage)
            index.index_document(segment, document_id=segment["file_checksum"])
            document_ids.append(segment["file_checksum"])
    else:
        index.index_document(gazette, document_id=gazette["file_checksum"])
        document_ids.append(gazette["file_checksum"])

    set_gazette_as_processed(gazette, database)
    return document_ids
 """
""" def new_try_process_gazette_filev1(
    gazette: Dict,
    territories: Iterable[Dict[str, Any]],
    database: DatabaseInterface,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
) -> List[str]:
    
    # Do all the work to extract the content from the gazette files
   
    logging.debug(f"Processing gazette {gazette['file_path']}")
    gazette_file = download_gazette_file(gazette, storage)
    
    # Extrair o texto da gazeta
    full_text = try_to_extract_content(gazette_file, text_extractor)
    gazette["url"] = define_file_url(gazette["file_path"])
    gazette_txt_path = define_gazette_txt_path(gazette)
    gazette["file_raw_txt"] = define_file_url(gazette_txt_path)
    
    # Upload do texto completo
    upload_raw_text(gazette_txt_path, full_text, storage)
    delete_gazette_files(gazette_file)
    
    document_ids = []
    if gazette_type_is_aggregated(gazette):
        # Processar documentos agregados
        segmenter = get_segmenter(gazette["territory_id"], territories)
        territory_segments = segmenter.get_gazette_segments(gazette)

        for segment in territory_segments:
            segment_txt_path = define_segment_txt_path(segment)
            segment["file_raw_txt"] = define_file_url(segment_txt_path)
            upload_raw_text(segment_txt_path, full_text, storage)
            index.index_document(segment, document_id=segment["file_checksum"])
            document_ids.append(segment["file_checksum"])
    else:
        # Processar documentos não agregados com segmentação de texto
        segments = segment_text(full_text, segment_size=5000)
        
        for i, segment in enumerate(segments):
            segment_data = gazette.copy()
            segment_data["source_text"] = segment  # Adiciona o trecho de texto
            segment_data["segment_index"] = i  # Índice do segmento
            segment_data["file_checksum"] = f"{gazette['file_checksum']}_{i}"  # Checksum único para cada segmento
            
            segment_txt_path = define_segment_txt_path(segment_data)
            upload_raw_text(segment_txt_path, segment, storage)
            
            index.index_document(segment_data, document_id=segment_data["file_checksum"])
            document_ids.append(segment_data["file_checksum"])

    set_gazette_as_processed(gazette, database)
    return document_ids """


""" def new_try_process_gazette_filev2(
    gazette: Dict,
    territories: Iterable[Dict[str, Any]],
    database: DatabaseInterface,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
) -> List[str]:
    
    # Do all the work to extract the content from the gazette files
   
    logging.debug(f"Processing gazette {gazette['file_path']}")
    gazette_file = download_gazette_file(gazette, storage)
    
    # Extrair o texto da gazeta
    full_text = try_to_extract_content(gazette_file, text_extractor)
    gazette["url"] = define_file_url(gazette["file_path"])
    gazette_txt_path = define_gazette_txt_path(gazette)
    gazette["file_raw_txt"] = define_file_url(gazette_txt_path)
    
    # Upload do texto completo
    upload_raw_text(gazette_txt_path, full_text, storage)
    delete_gazette_files(gazette_file)
    # print(f"Processing gazette {gazette['file_path']}")
    # print('Teste 3 new name:',Path(gazette["file_path"]).stem)
    # path = Path(file_path)

    # Obtenha o nome do arquivo sem a extensão
    # file_name_without_extension = path.stem
    # print('segment_size = 5000')

    gazette_filename = Path(gazette["file_path"]).stem
    document_ids = []
    if gazette_type_is_aggregated(gazette):
        # Processar documentos agregados
        segmenter = get_segmenter(gazette["territory_id"], territories)
        territory_segments = segmenter.get_gazette_segments(gazette)
        
        for segment in territory_segments:
            # Segmenta o texto unificado em partes menores de 2000 caracteres
            segments = segment_text(segment["source_text"], segment_size=5000)
            
            for i, segment_part in enumerate(segments):
                segment_data = segment.copy()
                segment_data["source_text"] = segment_part  # Adiciona o trecho de texto segmentado
                segment_data["segment_index"] = i  # Índice do segmento
                segment_data["file_checksum"] = f"{segment['file_checksum']}_{i}"  # Checksum único para cada segmento
                segment_data["file_path"] = f"{gazette_filename}_{i}" 
                
                segment_txt_path = define_segment_txt_path(segment_data)
                upload_raw_text(segment_txt_path, segment_part, storage)
                
                index.index_document(segment_data, document_id=segment_data["file_checksum"])
                document_ids.append(segment_data["file_checksum"])
    else:
        # Processar documentos não agregados com segmentação de texto
        segments = segment_text(full_text, segment_size=5000)
        print('type(segments)',type(segments))
        print('type(segments[0])',type(segments[0]))

        for i, segment in enumerate(segments):
            segment_data = gazette.copy()
            segment_data["source_text"] = segment  # Adiciona o trecho de texto
            segment_data["segment_index"] = i  # Índice do segmento
            segment_data["file_checksum"] = f"{gazette['file_checksum']}_{i}"  # Checksum único para cada segmento
            segment_data["file_path"] = f"{gazette_filename}_{i}"
            
            segment_txt_path = define_segment_txt_path(segment_data)
            upload_raw_text(segment_txt_path, segment, storage)
            
            index.index_document(segment_data, document_id=segment_data["file_checksum"])
            document_ids.append(segment_data["file_checksum"])
            

    set_gazette_as_processed(gazette, database)
    return document_ids """