<?php
namespace Raxon\Org\Node\Trait;

use Raxon\Org\Module\Core;
use Raxon\Org\Module\Controller;
use Raxon\Org\Module\Data as Storage;
use Raxon\Org\Module\File;
use Raxon\Org\Module\Filter as Module;
use Raxon\Org\Module\Parse;

use Exception;

use Raxon\Org\Exception\FileWriteException;
use Raxon\Org\Exception\ObjectException;

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