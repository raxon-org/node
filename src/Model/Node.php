<?php
namespace Raxon\Org\Node\Model;

use Raxon\Org\App;

use Raxon\Org\Module\Data as Storage;
use Raxon\Org\Module\Template\Main;

use Raxon\Org\Node\Trait\Data;
use Raxon\Org\Node\Trait\Role;

class Node extends Main {
    use Data;
    use Role;

    public function __construct(App $object){
        $this->object($object);
        $this->storage(new Storage());
    }
}