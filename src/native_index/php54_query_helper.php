<?php

class Cdx1QueryHelper
{
    private $cliPath;

    public function __construct($cliPath)
    {
        $this->cliPath = $cliPath;
    }

    public function query($indexDir, array $filters = array())
    {
        $cmd = array();
        $cmd[] = escapeshellarg($this->cliPath);
        $cmd[] = escapeshellarg($indexDir);

        if (!empty($filters['keywords'])) {
            $cmd[] = '--kw';
            $cmd[] = escapeshellarg(implode(',', $filters['keywords']));
        }
        if (!empty($filters['extensions'])) {
            $cmd[] = '--ext';
            $cmd[] = escapeshellarg(implode(',', $filters['extensions']));
        }
        if (!empty($filters['users'])) {
            $cmd[] = '--user';
            $cmd[] = escapeshellarg(implode(',', $filters['users']));
        }
        if (isset($filters['size_min'])) {
            $cmd[] = '--min';
            $cmd[] = escapeshellarg((string)$filters['size_min']);
        }
        if (isset($filters['size_max'])) {
            $cmd[] = '--max';
            $cmd[] = escapeshellarg((string)$filters['size_max']);
        }
        if (isset($filters['limit'])) {
            $cmd[] = '--limit';
            $cmd[] = escapeshellarg((string)$filters['limit']);
        }

        if (isset($filters['offset'])) {
            $cmd[] = '--offset';
            $cmd[] = escapeshellarg((string)$filters['offset']);
        }
        if (!empty($filters['sort'])) {
            $cmd[] = '--sort';
            $cmd[] = escapeshellarg((string)$filters['sort']);
        }
        if (!empty($filters['fields'])) {
            $cmd[] = '--fields';
            $cmd[] = escapeshellarg(implode(',', $filters['fields']));
        }

        $cmd[] = '--json';
        $cmd[] = '--docs';

        $fullCmd = implode(' ', $cmd) . ' 2>&1';
        $raw = shell_exec($fullCmd);
        if ($raw === null) {
            throw new Exception('cdx1_query execution failed');
        }

        $data = json_decode($raw, true);
        if (!is_array($data)) {
            throw new Exception('Invalid JSON from cdx1_query: ' . $raw);
        }

        return $data;
    }
}
