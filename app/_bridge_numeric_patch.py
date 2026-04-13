        return jsonify({'success': True, 'topics': data.get('topics', []),
                        'configured_topics': _cfg['mqtt'].get('topics', {})})