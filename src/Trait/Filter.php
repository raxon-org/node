<?php
namespace Raxon\Node\Trait;

use Raxon\Module\Core;
use Raxon\Module\Controller;
use Raxon\Module\Data as Storage;
use Raxon\Module\File;
use Raxon\Module\Filter as Module;
use Raxon\Module\Parse;

use Exception;

use Raxon\Exception\FileWriteException;
use Raxon\Exception\ObjectException;

trait Filter {

    /**
     * @throws Exception
     */
    private function filter($record=[], $filter=[], $options=[]): mixed
    {

        $list = [];
        $list[] = $record;
        $list = Module::list($list)->where($filter);
        if(!empty($list)){
            return $record;
        }
        return false;
    }
}