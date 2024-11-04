if gazette_type_is_aggregated(gazette):
        segmenter = get_segmenter(gazette["territory_id"], territories)
        territory_segments = segmenter.get_gazette_segments(gazette)

        for segment in territory_segments:
            segment_txt_path = define_segment_txt_path(segment)
            segment["file_raw_txt"] = define_file_url(segment_txt_path)
            upload_raw_text(segment_txt_path, segment["source_text"], storage)
            index.index_document(segment, document_id=segment["file_checksum"])
            document_ids.append(segment["file_checksum"])


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

