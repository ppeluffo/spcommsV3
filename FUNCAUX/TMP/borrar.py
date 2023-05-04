    # Leo la profundidad de las colas.
    from FUNCAUX.SERVICIOS import servicio_monitoreo
    servicio_mon = servicio_monitoreo.ServicioMonitoreo()
    d_in =  { 'REQUEST':'GET_QUEUE_LENGTH', 'DLGID':'', 'PARAMS':{'QNAME':'STATS_QUEUE'} }
    d_out = servicio_mon.process(d_in)
    res = d_out.get('RESULT',False)
    queue_length = d_out.get('PARAMS',{}).get('QUEUE_LENGTH', 0)
    if res:
        d_statistics['length_stats_queue'] = queue_length
    else:
        d_statistics['length_stats_queue'] = -1     
    #
    # Alerta por largo de cola
    # Si la cola crece mucho la borramos y alertamos !!!!
    if queue_length > 1000:
        d_log = { 'MODULE':__name__, 'FUNCTION':'STATS', 'LEVEL':'INFO', 'MSG':'ERROR: Redis STATS_QUEUE EXCEED LIMIT: erasing... !!!' }
        log2(d_log) 
        d_in =  { 'REQUEST':'DELETE_QUEUE', 'DLGID':'', 'PARAMS':{'QNAME':'STATS_QUEUE'} }
        _ = servicio_mon.process(d_in)



       # Guardo en REDIS
    servicio_mon = servicio_monitoreo.ServicioMonitoreo()
    d_in =  { 'REQUEST':'SAVE_STATS', 'DLGID':'', 'PARAMS':{'D_STATS': d_statistics} }
    _ = servicio_mon.process(d_in)
    #
    # Log
    msg = f'STATS: elapsed={d_statistics["duracion_frame"]:.04f},'
    msg += f'cfconf={d_statistics["count_frame_config_base"]},'
    msg += f'cfdata={d_statistics["count_frame_data"]},'
    msg += f'cRedis={ d_statistics["count_accesos_REDIS"]},'
    msg += f'cSql={d_statistics["count_accesos_SQL"]},'
    msg += f'QUEUE={d_statistics["length_stats_queue"]}'
    d_log = { 'MODULE':__name__, 'FUNCTION':'STATS', 'LEVEL':'INFO', 'MSG':msg }
    log2(d_log)
